pub struct ModelEntry {
    pub id: &'static str,
    pub repo: &'static str,
    pub filename: &'static str,
    pub sha256: &'static str,
    pub size_bytes: u64,
    pub license: &'static str,
    pub params: &'static str,
    pub quant: &'static str,
    /// Pre-bench estimate: GGUF file size in GiB plus 15%, rounded to one decimal.
    pub min_free_mem_gb: f32,
}

pub const REGISTRY: &[ModelEntry] = &[
    ModelEntry {
        id: "qwen3-coder-30b-a3b-q4",
        repo: "unsloth/Qwen3-Coder-30B-A3B-Instruct-GGUF",
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
        filename: "Qwen2.5-Coder-7B-Instruct-Q8_0.gguf",
        sha256: "3b879bf3c429aeb01ae5edec57eb2e787b24eb317991dfe491d69357c7f02735",
        size_bytes: 8_098_525_152,
        license: "apache-2.0",
        params: "7B",
        quant: "Q8_0",
        min_free_mem_gb: 8.7,
    },
    ModelEntry {
        id: "gemma-3-4b-it-q4",
        repo: "unsloth/gemma-3-4b-it-GGUF",
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
        filename: "Qwen3-14B-Q4_K_M.gguf",
        sha256: "5eaa0870bd81ed3b58a630a271234cfa604e43ffb3a19cd68e54a80dd9d52a66",
        size_bytes: 9_001_753_984,
        license: "apache-2.0",
        params: "14B",
        quant: "Q4_K_M",
        min_free_mem_gb: 9.6,
    },
];

pub fn find(id: &str) -> Option<&'static ModelEntry> {
    REGISTRY.iter().find(|entry| entry.id == id)
}

#[cfg(test)]
mod tests {
    use super::{find, REGISTRY};
    use std::collections::HashSet;

    const EXPECTED_IDS: &[&str] = &[
        "qwen3-coder-30b-a3b-q4",
        "qwen3-coder-30b-a3b-q8",
        "qwen25-coder-7b-q4",
        "qwen25-coder-7b-q8",
        "gemma-3-4b-it-q4",
        "qwen3-14b-q4",
    ];

    #[test]
    fn registry_contains_expected_six_entries() {
        assert_eq!(EXPECTED_IDS.len(), 6);
        assert_eq!(REGISTRY.len(), 6);
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
