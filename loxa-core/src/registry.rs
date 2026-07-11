pub struct ModelEntry {
    pub id: &'static str,
    pub repo: &'static str,
    pub revision: &'static str,
    pub filename: &'static str,
    pub sha256: &'static str,
    pub size_bytes: u64,
    pub license: &'static str,
    pub params: &'static str,
    pub quant: &'static str,
    /// Pre-bench estimate: GGUF file size in GiB plus 15%, rounded to one decimal.
    pub min_free_mem_gb: f32,
}

pub trait VerifiedModel {
    fn id(&self) -> &str;
    fn repo(&self) -> &str;
    fn revision(&self) -> &str {
        "main"
    }
    fn filename(&self) -> &str;
    fn sha256(&self) -> &str;
    fn size_bytes(&self) -> u64;
}

impl VerifiedModel for ModelEntry {
    fn id(&self) -> &str {
        self.id
    }
    fn repo(&self) -> &str {
        self.repo
    }
    fn revision(&self) -> &str {
        self.revision
    }
    fn filename(&self) -> &str {
        self.filename
    }
    fn sha256(&self) -> &str {
        self.sha256
    }
    fn size_bytes(&self) -> u64 {
        self.size_bytes
    }
}

#[derive(Clone, Debug, serde::Deserialize, PartialEq, serde::Serialize)]
pub struct UserModelEntry {
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
}

impl VerifiedModel for UserModelEntry {
    fn id(&self) -> &str {
        &self.id
    }
    fn repo(&self) -> &str {
        &self.repo
    }
    fn revision(&self) -> &str {
        &self.revision
    }
    fn filename(&self) -> &str {
        &self.filename
    }
    fn sha256(&self) -> &str {
        &self.sha256
    }
    fn size_bytes(&self) -> u64 {
        self.size_bytes
    }
}

impl UserModelEntry {
    pub fn validate(&self) -> Result<(), String> {
        let flat = !self.filename.is_empty()
            && self.filename.to_ascii_lowercase().ends_with(".gguf")
            && !self.filename.contains(['/', '\\']);
        let repo = self.repo.matches('/').count() == 1 && !self.repo.split('/').any(str::is_empty);
        let revision =
            self.revision.len() == 40 && self.revision.bytes().all(|b| b.is_ascii_hexdigit());
        let sha = self.sha256.len() == 64
            && self
                .sha256
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase());
        let id = !self.id.is_empty()
            && self
                .id
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
        let expected_memory =
            ((self.size_bytes as f64 / 1024_f64.powi(3)) * 1.15 * 10.0).round() as f32 / 10.0;
        if id
            && repo
            && revision
            && flat
            && sha
            && self.size_bytes > 0
            && !self.license.trim().is_empty()
            && !self.params.trim().is_empty()
            && !self.quant.trim().is_empty()
            && (self.min_free_mem_gb - expected_memory).abs() < f32::EPSILON
        {
            Ok(())
        } else {
            Err(format!("invalid user registry entry {}", self.id))
        }
    }
}

pub fn load_user_entries(dir: &std::path::Path) -> Result<Vec<UserModelEntry>, String> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut paths = std::fs::read_dir(dir)
        .map_err(|e| e.to_string())?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "json"))
        .collect::<Vec<_>>();
    paths.sort();
    let mut entries = Vec::new();
    for path in paths {
        let entry: UserModelEntry =
            serde_json::from_slice(&std::fs::read(&path).map_err(|e| e.to_string())?)
                .map_err(|e| format!("{}: {e}", path.display()))?;
        entry.validate()?;
        if REGISTRY.iter().any(|compiled| compiled.id == entry.id)
            || entries
                .iter()
                .any(|existing: &UserModelEntry| existing.id == entry.id)
        {
            return Err(format!("duplicate model id {}", entry.id));
        }
        entries.push(entry);
    }
    Ok(entries)
}

pub fn save_user_entry(
    dir: &std::path::Path,
    entry: &UserModelEntry,
) -> Result<std::path::PathBuf, String> {
    entry.validate()?;
    if REGISTRY.iter().any(|compiled| compiled.id == entry.id) {
        return Err(format!(
            "model id {} already exists; run loxa rm {} first",
            entry.id, entry.id
        ));
    }
    let existing = load_user_entries(dir)?;
    if existing.iter().any(|item| item.id == entry.id) {
        return Err(format!(
            "model id {} already exists; run loxa rm {} first",
            entry.id, entry.id
        ));
    }
    std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    let path = dir.join(format!("{}.json", entry.id));
    let bytes = serde_json::to_vec_pretty(entry).map_err(|e| e.to_string())?;
    std::fs::write(&path, bytes).map_err(|e| e.to_string())?;
    Ok(path)
}

pub const REGISTRY: &[ModelEntry] = &[
    ModelEntry {
        id: "qwen3-coder-30b-a3b-q4",
        repo: "unsloth/Qwen3-Coder-30B-A3B-Instruct-GGUF",
        revision: "main",
        filename: "Qwen3-Coder-30B-A3B-Instruct-Q4_K_M.gguf",
        sha256: "fadc3e5f8d42bf7e894a785b05082e47daee4df26680389817e2093056f088ad",
        size_bytes: 18_556_689_568,
        license: "apache-2.0",
        params: "30B-A3B",
        quant: "Q4_K_M",
        min_free_mem_gb: 19.9,
    },
    ModelEntry {
        id: "qwen3-coder-30b-a3b-q8",
        repo: "unsloth/Qwen3-Coder-30B-A3B-Instruct-GGUF",
        revision: "main",
        filename: "Qwen3-Coder-30B-A3B-Instruct-Q8_0.gguf",
        sha256: "4ff1cff607804037bf6d2168249c570baa4e1621292b159c0e06591e0d7c3066",
        size_bytes: 32_483_935_392,
        license: "apache-2.0",
        params: "30B-A3B",
        quant: "Q8_0",
        min_free_mem_gb: 34.8,
    },
    ModelEntry {
        id: "qwen25-coder-7b-q4",
        repo: "unsloth/Qwen2.5-Coder-7B-Instruct-GGUF",
        revision: "main",
        filename: "Qwen2.5-Coder-7B-Instruct-Q4_K_M.gguf",
        sha256: "9a961bb225cb2b9fd84b2297df0d53089895c049d7d9dc5f5f8aebbcd3247872",
        size_bytes: 4_683_073_504,
        license: "apache-2.0",
        params: "7B",
        quant: "Q4_K_M",
        min_free_mem_gb: 5.0,
    },
    ModelEntry {
        id: "qwen25-coder-7b-q8",
        repo: "unsloth/Qwen2.5-Coder-7B-Instruct-GGUF",
        revision: "main",
        filename: "Qwen2.5-Coder-7B-Instruct-Q8_0.gguf",
        sha256: "3b879bf3c429aeb01ae5edec57eb2e787b24eb317991dfe491d69357c7f02735",
        size_bytes: 8_098_525_152,
        license: "apache-2.0",
        params: "7B",
        quant: "Q8_0",
        min_free_mem_gb: 8.7,
    },
    ModelEntry {
        // Verified recipe retained, but Gemma 3 is wrong for native tool use.
        id: "gemma-3-4b-it-q4",
        repo: "unsloth/gemma-3-4b-it-GGUF",
        revision: "main",
        filename: "gemma-3-4b-it-Q4_K_M.gguf",
        sha256: "04a43a22e8d2003deda5acc262f68ec1005fa76c735a9962a8c77042a74a7d19",
        size_bytes: 2_489_894_016,
        license: "gemma",
        params: "4B",
        quant: "Q4_K_M",
        min_free_mem_gb: 2.7,
    },
    ModelEntry {
        id: "qwen3-14b-q4",
        repo: "unsloth/Qwen3-14B-GGUF",
        revision: "main",
        filename: "Qwen3-14B-Q4_K_M.gguf",
        sha256: "5eaa0870bd81ed3b58a630a271234cfa604e43ffb3a19cd68e54a80dd9d52a66",
        size_bytes: 9_001_753_984,
        license: "apache-2.0",
        params: "14B",
        quant: "Q4_K_M",
        min_free_mem_gb: 9.6,
    },
    ModelEntry {
        id: "gemma-4-e4b-it-q4",
        repo: "unsloth/gemma-4-E4B-it-GGUF",
        revision: "0720adb23527c2cd5ea01d1db067cd960327fdac",
        filename: "gemma-4-E4B-it-Q4_K_M.gguf",
        sha256: "519b9793ed6ce0ff530f1b7c96e848e08e49e7af4d57bb97f76215963a54146d",
        size_bytes: 4_977_169_568,
        license: "apache-2.0",
        params: "E4B",
        quant: "Q4_K_M",
        min_free_mem_gb: 5.3,
    },
];

pub fn find(id: &str) -> Option<&'static ModelEntry> {
    if let Some(entry) = REGISTRY.iter().find(|entry| entry.id == id) {
        return Some(entry);
    }
    static USERS: std::sync::OnceLock<Vec<ModelEntry>> = std::sync::OnceLock::new();
    USERS
        .get_or_init(|| {
            let dir = std::env::var_os("HOME")
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| ".".into())
                .join(".loxa/registry.d");
            load_user_entries(&dir)
                .unwrap_or_default()
                .into_iter()
                .map(|entry| ModelEntry {
                    id: Box::leak(entry.id.into_boxed_str()),
                    repo: Box::leak(entry.repo.into_boxed_str()),
                    revision: Box::leak(entry.revision.into_boxed_str()),
                    filename: Box::leak(entry.filename.into_boxed_str()),
                    sha256: Box::leak(entry.sha256.into_boxed_str()),
                    size_bytes: entry.size_bytes,
                    license: Box::leak(entry.license.into_boxed_str()),
                    params: Box::leak(entry.params.into_boxed_str()),
                    quant: Box::leak(entry.quant.into_boxed_str()),
                    min_free_mem_gb: entry.min_free_mem_gb,
                })
                .collect()
        })
        .iter()
        .find(|entry| entry.id == id)
}

#[cfg(test)]
mod tests {
    use super::{find, load_user_entries, save_user_entry, UserModelEntry, REGISTRY};
    use std::collections::HashSet;

    fn user_entry() -> UserModelEntry {
        UserModelEntry {
            id: "demo-q4-k-m".into(),
            repo: "owner/repo".into(),
            revision: "0123456789abcdef0123456789abcdef01234567".into(),
            filename: "demo-Q4_K_M.gguf".into(),
            sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            size_bytes: 100 * 1024 * 1024,
            license: "apache-2.0".into(),
            params: "unknown".into(),
            quant: "Q4_K_M".into(),
            min_free_mem_gb: 0.1,
        }
    }

    #[test]
    fn user_registry_round_trips_and_refuses_collision() {
        let temp = tempfile::tempdir().unwrap();
        let entry = user_entry();
        save_user_entry(temp.path(), &entry).unwrap();
        assert_eq!(load_user_entries(temp.path()).unwrap(), vec![entry.clone()]);
        assert!(save_user_entry(temp.path(), &entry)
            .unwrap_err()
            .contains("already exists"));
    }

    #[test]
    fn user_registry_rejects_unpinned_or_unverified_entries() {
        let mut entry = user_entry();
        entry.revision = "main".into();
        assert!(entry.validate().is_err());
        entry.revision = "0123456789abcdef0123456789abcdef01234567".into();
        entry.sha256 = "TODO_VERIFY".into();
        assert!(entry.validate().is_err());
    }

    const EXPECTED_IDS: &[&str] = &[
        "qwen3-coder-30b-a3b-q4",
        "qwen3-coder-30b-a3b-q8",
        "qwen25-coder-7b-q4",
        "qwen25-coder-7b-q8",
        "gemma-3-4b-it-q4",
        "qwen3-14b-q4",
        "gemma-4-e4b-it-q4",
    ];

    #[test]
    fn registry_contains_expected_seven_entries() {
        assert_eq!(EXPECTED_IDS.len(), 7);
        assert_eq!(REGISTRY.len(), 7);
    }

    #[test]
    fn registry_ids_are_unique() {
        let ids = REGISTRY
            .iter()
            .map(|entry| entry.id)
            .collect::<HashSet<_>>();

        assert_eq!(ids.len(), REGISTRY.len());
    }

    #[test]
    fn registry_contains_exact_expected_ids() {
        let actual_ids = REGISTRY
            .iter()
            .map(|entry| entry.id)
            .collect::<HashSet<_>>();
        let expected_ids = EXPECTED_IDS.iter().copied().collect::<HashSet<_>>();

        assert_eq!(actual_ids, expected_ids);
    }

    #[test]
    fn find_returns_each_registry_entry_and_none_for_unknown() {
        for entry in REGISTRY {
            let found =
                find(entry.id).unwrap_or_else(|| panic!("missing registry id {}", entry.id));

            assert_eq!(found.id, entry.id);
            assert_eq!(found.repo, entry.repo);
            assert_eq!(found.filename, entry.filename);
            assert_eq!(found.sha256, entry.sha256);
        }

        assert!(find("unknown-model").is_none());
    }

    #[test]
    fn sha256_values_are_verified_hashes_or_todo_markers() {
        for entry in REGISTRY {
            let valid_hash = entry.sha256.len() == 64
                && entry
                    .sha256
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));

            assert!(
                valid_hash || entry.sha256 == "TODO_VERIFY",
                "invalid sha256 for {}",
                entry.id
            );
        }
    }

    #[test]
    fn registry_contains_no_todo_verify_hashes() {
        for entry in REGISTRY {
            assert_ne!(
                entry.sha256, "TODO_VERIFY",
                "unverified hash for {}",
                entry.id
            );
        }
    }

    #[test]
    fn repos_are_owner_and_name_pairs() {
        for entry in REGISTRY {
            assert_eq!(
                entry.repo.matches('/').count(),
                1,
                "repo must contain exactly one slash: {}",
                entry.repo
            );
        }
    }

    #[test]
    fn filenames_are_flat_gguf_names() {
        for entry in REGISTRY {
            assert!(
                is_flat_filename(entry.filename),
                "filename is not downloader-compatible: {}",
                entry.filename
            );
            assert!(
                entry.filename.ends_with(".gguf"),
                "filename must end with .gguf: {}",
                entry.filename
            );
        }
    }

    #[test]
    fn sizes_are_larger_than_one_gib() {
        for entry in REGISTRY {
            assert!(
                entry.size_bytes > 1_073_741_824,
                "size must be larger than 1 GiB: {}",
                entry.id
            );
        }
    }

    #[test]
    fn min_free_memory_is_size_plus_fifteen_percent_rounded_to_one_decimal() {
        for entry in REGISTRY {
            let expected =
                ((entry.size_bytes as f64 / 1_073_741_824.0) * 1.15 * 10.0).round() / 10.0;

            assert!(
                entry.min_free_mem_gb > 0.0,
                "min_free_mem_gb must be positive: {}",
                entry.id
            );
            assert!(
                (entry.min_free_mem_gb as f64 - expected).abs() <= 0.1,
                "min_free_mem_gb mismatch for {}: got {}, expected {}",
                entry.id,
                entry.min_free_mem_gb,
                expected
            );
        }
    }

    fn is_flat_filename(filename: &str) -> bool {
        !filename.is_empty()
            && filename == filename.trim()
            && filename != "."
            && filename != ".."
            && !filename.ends_with('.')
            && !filename.contains('/')
            && !filename.contains('\\')
            && !filename.contains('\0')
    }
}
