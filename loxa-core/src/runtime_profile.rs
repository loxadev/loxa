use crate::registry::VerifiedModel;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PinnedArtifact {
    pub id: &'static str,
    pub repo: &'static str,
    pub revision: &'static str,
    pub filename: &'static str,
    pub sha256: &'static str,
    pub size_bytes: u64,
}

impl VerifiedModel for PinnedArtifact {
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LlamaRuntimeProfile {
    pub model_id: &'static str,
    pub target: PinnedArtifact,
    pub drafter: PinnedArtifact,
    pub ctx_size: u32,
    pub spec_type: &'static str,
    pub draft_n_max: u32,
    pub jinja: bool,
}

const GEMMA_4_MTP_PROFILE: LlamaRuntimeProfile = LlamaRuntimeProfile {
    model_id: "loxa",
    target: PinnedArtifact {
        id: "loxa",
        repo: "unsloth/gemma-4-12B-it-qat-GGUF",
        revision: "980b060c40a8539ac159e0501a3e0f66a6365af3",
        filename: "gemma-4-12B-it-qat-UD-Q4_K_XL.gguf",
        sha256: "90fd44e29e0d7cffeb0fd00dc73cfdab9ed0b0e95306ecf7821ea634c940c370",
        size_bytes: 6_716_356_800,
    },
    drafter: PinnedArtifact {
        id: "loxa-mtp-drafter",
        repo: "unsloth/gemma-4-12B-it-qat-GGUF",
        revision: "980b060c40a8539ac159e0501a3e0f66a6365af3",
        filename: "mtp-gemma-4-12B-it.gguf",
        sha256: "fcb35dea42c71333db904cee11baac525c9ef872818ee3753f6cb156f3c6f4f6",
        size_bytes: 253_708_800,
    },
    ctx_size: 8192,
    spec_type: "draft-mtp",
    draft_n_max: 4,
    jinja: true,
};

pub fn runtime_profile(model_id: &str) -> Option<&'static LlamaRuntimeProfile> {
    (model_id == GEMMA_4_MTP_PROFILE.model_id).then_some(&GEMMA_4_MTP_PROFILE)
}

#[cfg(test)]
mod tests {
    use super::runtime_profile;
    use crate::registry::VerifiedModel;

    const REVISION: &str = "980b060c40a8539ac159e0501a3e0f66a6365af3";

    #[test]
    fn loxa_resolves_to_the_qualified_gemma_4_mtp_pair() {
        let profile = runtime_profile("loxa").expect("loxa profile");

        assert_eq!(profile.model_id, "loxa");
        assert_eq!(profile.target.id(), "loxa");
        assert_eq!(profile.target.repo(), "unsloth/gemma-4-12B-it-qat-GGUF");
        assert_eq!(profile.target.revision(), REVISION);
        assert_eq!(
            profile.target.filename(),
            "gemma-4-12B-it-qat-UD-Q4_K_XL.gguf"
        );
        assert_eq!(
            profile.target.sha256(),
            "90fd44e29e0d7cffeb0fd00dc73cfdab9ed0b0e95306ecf7821ea634c940c370"
        );
        assert_eq!(profile.target.size_bytes(), 6_716_356_800);

        assert_eq!(profile.drafter.id(), "loxa-mtp-drafter");
        assert_eq!(profile.drafter.repo(), "unsloth/gemma-4-12B-it-qat-GGUF");
        assert_eq!(profile.drafter.revision(), REVISION);
        assert_eq!(profile.drafter.filename(), "mtp-gemma-4-12B-it.gguf");
        assert_eq!(
            profile.drafter.sha256(),
            "fcb35dea42c71333db904cee11baac525c9ef872818ee3753f6cb156f3c6f4f6"
        );
        assert_eq!(profile.drafter.size_bytes(), 253_708_800);

        assert_eq!(profile.ctx_size, 8192);
        assert_eq!(profile.spec_type, "draft-mtp");
        assert_eq!(profile.draft_n_max, 4);
        assert!(profile.jinja);
    }

    #[test]
    fn qualified_artifacts_are_immutable_flat_gguf_files() {
        let profile = runtime_profile("loxa").expect("loxa profile");

        for artifact in [&profile.target, &profile.drafter] {
            assert_eq!(artifact.revision(), REVISION);
            assert_eq!(artifact.revision().len(), 40);
            assert!(artifact
                .revision()
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()));
            assert!(artifact.filename().ends_with(".gguf"));
            assert!(!artifact.filename().contains(['/', '\\']));
            assert_eq!(artifact.sha256().len(), 64);
            assert!(artifact
                .sha256()
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
            assert!(artifact.size_bytes() > 0);
        }

        assert!(runtime_profile("unknown-model").is_none());
    }
}
