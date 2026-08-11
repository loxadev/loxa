use loxa::catalog::{
    Artifact, ArtifactProvenance, ArtifactRole, Manifest, RuntimeQualification,
    GEMMA4_BUNDLED_LLAMA_BUILD, GEMMA4_DRAFT_SHA256, GEMMA4_DRAFT_SIZE, GEMMA4_LEGACY_LLAMA_BUILD,
    GEMMA4_MODEL_SHA256, GEMMA4_MODEL_SIZE, GEMMA4_MTP_PROFILE,
};
use loxa::runtime_identity::RuntimeIdentity;

fn exact_manifest(build: &str) -> Manifest {
    Manifest {
        version: 3,
        id: "gemma-4-12b-it-qat-ud-q4-k-xl".into(),
        repo: None,
        revision: None,
        remote_filename: None,
        origin: None,
        source_filename: None,
        local_filename: "model.gguf".into(),
        sha256: GEMMA4_MODEL_SHA256.into(),
        size: GEMMA4_MODEL_SIZE,
        artifacts: Some(vec![
            Artifact {
                role: ArtifactRole::Model,
                local_filename: "model.gguf".into(),
                sha256: GEMMA4_MODEL_SHA256.into(),
                size: GEMMA4_MODEL_SIZE,
                provenance: ArtifactProvenance::Local {
                    source_filename: "target.gguf".into(),
                },
            },
            Artifact {
                role: ArtifactRole::Draft,
                local_filename: "draft.gguf".into(),
                sha256: GEMMA4_DRAFT_SHA256.into(),
                size: GEMMA4_DRAFT_SIZE,
                provenance: ArtifactProvenance::Local {
                    source_filename: "draft-source.gguf".into(),
                },
            },
        ]),
        profile: Some(GEMMA4_MTP_PROFILE.into()),
        runtime: Some(RuntimeQualification {
            engine: "llama.cpp".into(),
            build: build.into(),
        }),
    }
}

#[test]
fn exact_legacy_and_current_provenance_are_closed_compatible_rows_for_either_active_runtime() {
    for build in [GEMMA4_LEGACY_LLAMA_BUILD, GEMMA4_BUNDLED_LLAMA_BUILD] {
        let manifest = exact_manifest(build);
        manifest.validate().unwrap();
        assert!(RuntimeIdentity::LegacyCliB10121.supports_manifest(&manifest));
        assert!(RuntimeIdentity::BundledB10344.supports_manifest(&manifest));
    }
}

#[test]
fn any_profile_artifact_or_build_mutation_closes_legacy_compatibility() {
    let exact = exact_manifest(GEMMA4_LEGACY_LLAMA_BUILD);

    let mut changed_build = exact.clone();
    changed_build.runtime.as_mut().unwrap().build = "b10122".into();
    let mut changed_profile = exact.clone();
    changed_profile.profile = Some("gemma4-mtp-v2".into());
    let mut changed_hash = exact.clone();
    changed_hash.artifacts.as_mut().unwrap()[1].sha256 = "0".repeat(64);
    let mut changed_size = exact;
    changed_size.artifacts.as_mut().unwrap()[0].size -= 1;

    for changed in [changed_build, changed_profile, changed_hash, changed_size] {
        assert!(changed.validate().is_err());
        assert!(!RuntimeIdentity::BundledB10344.supports_manifest(&changed));
        assert!(!RuntimeIdentity::LegacyCliB10121.supports_manifest(&changed));
    }
}

#[test]
fn neither_production_runtime_admits_a_qualified_manifest_without_its_exact_draft() {
    for build in [GEMMA4_LEGACY_LLAMA_BUILD, GEMMA4_BUNDLED_LLAMA_BUILD] {
        let mut manifest = exact_manifest(build);
        manifest.artifacts.as_mut().unwrap().pop();

        assert_eq!(
            manifest.validate().unwrap_err(),
            "qualified production bundle is missing its draft artifact"
        );
        assert!(!RuntimeIdentity::LegacyCliB10121.supports_manifest(&manifest));
        assert!(!RuntimeIdentity::BundledB10344.supports_manifest(&manifest));
    }
}
