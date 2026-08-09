use crate::catalog::{ArtifactRef, Manifest};
use serde::{Deserialize, Serialize};

const FINGERPRINT_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum EffectiveProfile {
    Generic,
    Gemma4Mtp,
    PrimaryOnly,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ArtifactFingerprint {
    local_filename: String,
    sha256: String,
    size: u64,
}

impl From<ArtifactRef<'_>> for ArtifactFingerprint {
    fn from(artifact: ArtifactRef<'_>) -> Self {
        Self {
            local_filename: artifact.local_filename.to_owned(),
            sha256: artifact.sha256.to_ascii_lowercase(),
            size: artifact.size,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RuntimeFingerprint {
    schema_version: u32,
    model_id: String,
    effective_context: u32,
    effective_profile: EffectiveProfile,
    sleep_policy: Option<u64>,
    primary: ArtifactFingerprint,
    draft: Option<ArtifactFingerprint>,
}

impl<'de> Deserialize<'de> for RuntimeFingerprint {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WireFingerprint {
            schema_version: u32,
            model_id: String,
            effective_context: u32,
            effective_profile: EffectiveProfile,
            sleep_policy: Option<u64>,
            primary: ArtifactFingerprint,
            draft: Option<ArtifactFingerprint>,
        }

        let wire = WireFingerprint::deserialize(deserializer)?;
        let mut fingerprint = Self {
            schema_version: wire.schema_version,
            model_id: wire.model_id,
            effective_context: wire.effective_context,
            effective_profile: wire.effective_profile,
            sleep_policy: wire.sleep_policy,
            primary: wire.primary,
            draft: wire.draft,
        };
        fingerprint.primary.sha256.make_ascii_lowercase();
        if let Some(draft) = &mut fingerprint.draft {
            draft.sha256.make_ascii_lowercase();
        }
        fingerprint.validate().map_err(serde::de::Error::custom)?;
        Ok(fingerprint)
    }
}

impl RuntimeFingerprint {
    fn validate(&self) -> Result<(), String> {
        if self.schema_version != FINGERPRINT_SCHEMA_VERSION {
            return Err("unsupported runtime fingerprint schema".into());
        }
        crate::paths::validate_id(&self.model_id)?;
        if !matches!(self.sleep_policy, None | Some(300)) {
            return Err("unsupported runtime fingerprint sleep policy".into());
        }
        validate_artifact(&self.primary)?;
        if let Some(draft) = &self.draft {
            validate_artifact(draft)?;
        }
        match (
            self.effective_profile,
            self.draft.is_some(),
            self.sleep_policy,
        ) {
            (EffectiveProfile::Generic, false, _)
            | (EffectiveProfile::Gemma4Mtp, true, _)
            | (EffectiveProfile::PrimaryOnly, false, Some(300)) => Ok(()),
            _ => Err("runtime fingerprint profile contradicts its draft artifact".into()),
        }
    }

    pub(crate) fn from_manifest(
        manifest: &Manifest,
        effective_context: u32,
        effective_profile: EffectiveProfile,
        sleep_policy: Option<u64>,
    ) -> Result<Self, String> {
        if effective_profile == EffectiveProfile::PrimaryOnly {
            return Err("primary-only fingerprints must come from the paired fallback".into());
        }
        let fingerprint = Self {
            schema_version: FINGERPRINT_SCHEMA_VERSION,
            model_id: manifest.id.clone(),
            effective_context,
            effective_profile,
            sleep_policy,
            primary: manifest.primary_artifact().into(),
            draft: manifest.draft_artifact().map(Into::into),
        };
        fingerprint.validate()?;
        Ok(fingerprint)
    }

    pub(crate) fn primary_only(&self) -> Option<Self> {
        if self.effective_profile != EffectiveProfile::Gemma4Mtp
            || self.draft.is_none()
            || self.sleep_policy != Some(300)
        {
            return None;
        }
        let mut primary_only = self.clone();
        primary_only.effective_profile = EffectiveProfile::PrimaryOnly;
        primary_only.draft = None;
        Some(primary_only)
    }

    pub(crate) fn validate_persistent_lease(&self, model_id: &str) -> Result<(), String> {
        self.validate()?;
        if self.model_id != model_id {
            return Err("runtime lease model contradicts its fingerprint".into());
        }
        if self.sleep_policy != Some(300) {
            return Err("persistent runtime fingerprint requires sleep policy 300".into());
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn effective_profile(&self) -> EffectiveProfile {
        self.effective_profile
    }

    #[cfg(test)]
    pub(crate) fn sleep_policy(&self) -> Option<u64> {
        self.sleep_policy
    }

    #[cfg(test)]
    pub(crate) fn draft(&self) -> Option<&ArtifactFingerprint> {
        self.draft.as_ref()
    }
}

fn validate_artifact(artifact: &ArtifactFingerprint) -> Result<(), String> {
    let filename = &artifact.local_filename;
    if filename.is_empty()
        || filename == "."
        || filename.contains("..")
        || filename.contains(['/', '\\'])
        || filename.chars().any(char::is_control)
    {
        return Err("runtime fingerprint artifact filename is not a safe basename".into());
    }
    if artifact.sha256.len() != 64
        || !artifact
            .sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err("runtime fingerprint artifact SHA-256 must be lowercase hexadecimal".into());
    }
    if artifact.size == 0 {
        return Err("runtime fingerprint artifact size must be positive".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Manifest;

    fn artifact(filename: &str, sha256: &str, size: u64) -> serde_json::Value {
        serde_json::json!({
            "local_filename": filename,
            "sha256": sha256,
            "size": size,
        })
    }

    fn generic_fingerprint() -> serde_json::Value {
        serde_json::json!({
            "schema_version": 1,
            "model_id": "demo",
            "effective_context": 4096,
            "effective_profile": "generic",
            "sleep_policy": null,
            "primary": artifact("model.gguf", &"a".repeat(64), 1),
            "draft": null,
        })
    }

    fn generic_manifest() -> Manifest {
        serde_json::from_value(serde_json::json!({
            "version": 1,
            "id": "demo",
            "repo": "owner/repo",
            "revision": "0".repeat(40),
            "remote_filename": "model.gguf",
            "local_filename": "model.gguf",
            "sha256": "a".repeat(64),
            "size": 1,
        }))
        .unwrap()
    }

    fn mtp_manifest() -> Manifest {
        serde_json::from_value(serde_json::json!({
            "version": 3,
            "id": "demo",
            "local_filename": "model.gguf",
            "sha256": "a".repeat(64),
            "size": 1,
            "artifacts": [
                {
                    "role": "model",
                    "local_filename": "model.gguf",
                    "sha256": "a".repeat(64),
                    "size": 1,
                    "provenance": {
                        "type": "local",
                        "source_filename": "model-source.gguf"
                    }
                },
                {
                    "role": "draft",
                    "local_filename": "draft.gguf",
                    "sha256": "b".repeat(64),
                    "size": 1,
                    "provenance": {
                        "type": "local",
                        "source_filename": "draft-source.gguf"
                    }
                }
            ],
            "profile": crate::catalog::TEST_MTP_PROFILE,
            "runtime": {
                "engine": "llama.cpp",
                "build": crate::catalog::TEST_LLAMA_BUILD
            }
        }))
        .unwrap()
    }

    #[test]
    fn deserialization_rejects_every_invalid_fingerprint_invariant() {
        let mut unsupported_schema = generic_fingerprint();
        unsupported_schema["schema_version"] = serde_json::json!(2);
        let mut empty_filename = generic_fingerprint();
        empty_filename["primary"]["local_filename"] = serde_json::json!("");
        let mut traversal_filename = generic_fingerprint();
        traversal_filename["primary"]["local_filename"] = serde_json::json!("../model.gguf");
        let mut nested_filename = generic_fingerprint();
        nested_filename["primary"]["local_filename"] = serde_json::json!("nested/model.gguf");
        let mut backslash_filename = generic_fingerprint();
        backslash_filename["primary"]["local_filename"] = serde_json::json!("nested\\model.gguf");
        let mut empty_model_id = generic_fingerprint();
        empty_model_id["model_id"] = serde_json::json!("");
        let mut traversal_model_id = generic_fingerprint();
        traversal_model_id["model_id"] = serde_json::json!("../demo");
        let mut uppercase_model_id = generic_fingerprint();
        uppercase_model_id["model_id"] = serde_json::json!("Demo");
        let mut control_model_id = generic_fingerprint();
        control_model_id["model_id"] = serde_json::json!("demo\n");
        let mut oversized_model_id = generic_fingerprint();
        oversized_model_id["model_id"] = serde_json::json!("a".repeat(121));
        let mut short_sha = generic_fingerprint();
        short_sha["primary"]["sha256"] = serde_json::json!("a".repeat(63));
        let mut non_hex_sha = generic_fingerprint();
        non_hex_sha["primary"]["sha256"] = serde_json::json!("g".repeat(64));
        let mut zero_primary_size = generic_fingerprint();
        zero_primary_size["primary"]["size"] = serde_json::json!(0);
        let mut unsupported_sleep = generic_fingerprint();
        unsupported_sleep["sleep_policy"] = serde_json::json!(301);
        let mut generic_with_draft = generic_fingerprint();
        generic_with_draft["draft"] = artifact("draft.gguf", &"b".repeat(64), 1);
        let mut primary_only_with_draft = generic_with_draft.clone();
        primary_only_with_draft["effective_profile"] = serde_json::json!("primary_only");
        primary_only_with_draft["sleep_policy"] = serde_json::json!(300);
        let mut primary_only_without_sleep = generic_fingerprint();
        primary_only_without_sleep["effective_profile"] = serde_json::json!("primary_only");
        let mut mtp_without_draft = generic_fingerprint();
        mtp_without_draft["effective_profile"] = serde_json::json!("gemma4_mtp");
        let mut zero_draft_size = generic_with_draft.clone();
        zero_draft_size["effective_profile"] = serde_json::json!("gemma4_mtp");
        zero_draft_size["draft"]["size"] = serde_json::json!(0);

        for (name, value) in [
            ("unsupported schema", unsupported_schema),
            ("empty filename", empty_filename),
            ("traversal filename", traversal_filename),
            ("nested filename", nested_filename),
            ("backslash filename", backslash_filename),
            ("empty model ID", empty_model_id),
            ("traversal model ID", traversal_model_id),
            ("uppercase model ID", uppercase_model_id),
            ("control model ID", control_model_id),
            ("oversized model ID", oversized_model_id),
            ("short SHA-256", short_sha),
            ("non-hex SHA-256", non_hex_sha),
            ("zero primary size", zero_primary_size),
            ("unsupported sleep policy", unsupported_sleep),
            ("generic with draft", generic_with_draft),
            ("primary-only with draft", primary_only_with_draft),
            (
                "primary-only without persistent sleep",
                primary_only_without_sleep,
            ),
            ("MTP without draft", mtp_without_draft),
            ("zero draft size", zero_draft_size),
        ] {
            assert!(
                serde_json::from_value::<RuntimeFingerprint>(value).is_err(),
                "accepted {name}"
            );
        }
    }

    #[test]
    fn deserialization_accepts_each_coherent_profile() {
        let generic = generic_fingerprint();
        let mut primary_only = generic.clone();
        primary_only["effective_profile"] = serde_json::json!("primary_only");
        primary_only["sleep_policy"] = serde_json::json!(300);
        let mut mtp = generic;
        mtp["effective_profile"] = serde_json::json!("gemma4_mtp");
        mtp["sleep_policy"] = serde_json::json!(300);
        mtp["draft"] = artifact("draft.gguf", &"b".repeat(64), 1);

        for (name, value) in [
            ("generic", generic_fingerprint()),
            ("primary-only", primary_only),
            ("MTP", mtp),
        ] {
            assert!(
                serde_json::from_value::<RuntimeFingerprint>(value).is_ok(),
                "rejected coherent {name} fingerprint"
            );
        }
    }

    #[test]
    fn wire_preserves_zero_context_and_canonicalizes_uppercase_digests() {
        let mut wire = generic_fingerprint();
        wire["effective_context"] = serde_json::json!(0);
        wire["effective_profile"] = serde_json::json!("gemma4_mtp");
        wire["sleep_policy"] = serde_json::json!(300);
        wire["primary"]["sha256"] = serde_json::json!("A".repeat(64));
        wire["draft"] = artifact("draft.gguf", &"B".repeat(64), 1);

        let fingerprint = serde_json::from_value::<RuntimeFingerprint>(wire).unwrap();
        let canonical = serde_json::to_value(&fingerprint).unwrap();

        assert_eq!(canonical["effective_context"], 0);
        assert_eq!(canonical["primary"]["sha256"], "a".repeat(64));
        assert_eq!(canonical["draft"]["sha256"], "b".repeat(64));
    }

    #[test]
    fn admitted_construction_rejects_invalid_values_and_direct_primary_only() {
        let generic = generic_manifest();
        let mtp = mtp_manifest();
        let mut unsafe_filename = generic.clone();
        unsafe_filename.local_filename = "../model.gguf".into();
        let mut zero_size = generic.clone();
        zero_size.size = 0;
        let mut mtp_without_draft = mtp.clone();
        mtp_without_draft
            .artifacts
            .as_mut()
            .unwrap()
            .retain(|artifact| artifact.role != crate::catalog::ArtifactRole::Draft);

        assert!(
            RuntimeFingerprint::from_manifest(&generic, 4096, EffectiveProfile::Generic, None,)
                .is_ok()
        );
        assert!(RuntimeFingerprint::from_manifest(
            &mtp,
            8192,
            EffectiveProfile::Gemma4Mtp,
            Some(300),
        )
        .is_ok());
        let mut uppercase_sha = generic.clone();
        uppercase_sha.sha256.make_ascii_uppercase();
        let canonical =
            RuntimeFingerprint::from_manifest(&uppercase_sha, 0, EffectiveProfile::Generic, None)
                .unwrap();
        let canonical = serde_json::to_value(canonical).unwrap();
        assert_eq!(canonical["effective_context"], 0);
        assert_eq!(canonical["primary"]["sha256"], "a".repeat(64));

        for (name, manifest, context, profile, sleep_policy) in [
            (
                "unsafe filename",
                unsafe_filename,
                4096,
                EffectiveProfile::Generic,
                None,
            ),
            (
                "zero size",
                zero_size,
                4096,
                EffectiveProfile::Generic,
                None,
            ),
            (
                "unsupported sleep policy",
                generic.clone(),
                4096,
                EffectiveProfile::Generic,
                Some(301),
            ),
            (
                "generic with draft",
                mtp.clone(),
                8192,
                EffectiveProfile::Generic,
                Some(300),
            ),
            (
                "MTP without draft",
                mtp_without_draft,
                8192,
                EffectiveProfile::Gemma4Mtp,
                Some(300),
            ),
            (
                "direct primary-only",
                generic,
                4096,
                EffectiveProfile::PrimaryOnly,
                Some(300),
            ),
        ] {
            assert!(
                RuntimeFingerprint::from_manifest(&manifest, context, profile, sleep_policy)
                    .is_err(),
                "constructed {name}"
            );
        }
    }

    #[test]
    fn primary_only_transformation_requires_persistent_mtp_sleep_policy() {
        let foreground_mtp = RuntimeFingerprint::from_manifest(
            &mtp_manifest(),
            8192,
            EffectiveProfile::Gemma4Mtp,
            None,
        )
        .unwrap();

        assert!(foreground_mtp.primary_only().is_none());
    }
}
