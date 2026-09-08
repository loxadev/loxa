use crate::catalog::{ArtifactRef, Manifest};
use serde::{Deserialize, Serialize};

const FINGERPRINT_SCHEMA_VERSION: u32 = 1;
const SERVICE_FINGERPRINT_SCHEMA_VERSION: u32 = 2;
pub(crate) const PERSISTENT_SLEEP_IDLE_SECONDS: u64 = 300;
pub(crate) const SERVICE_MIN_CONTEXT: u32 = 512;
pub(crate) const SERVICE_MAX_CONTEXT: u32 = 32_768;
// Prior schema-1 leases must remain decodable so recovery can terminate their exact child.
const LEGACY_PERSISTENT_SLEEP_IDLE_SECONDS: u64 = 60;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum EffectiveProfile {
    Generic,
    Gemma4Mtp,
    PrimaryOnly,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ServiceRuntimeProfile {
    pub(crate) max_output_tokens: u32,
    pub(crate) batch_size: u32,
    pub(crate) micro_batch_size: u32,
    pub(crate) threads: u16,
    pub(crate) batch_threads: u16,
    pub(crate) cache_type_k: String,
    pub(crate) cache_type_v: String,
    pub(crate) kv_offload: bool,
    pub(crate) gpu_layers: String,
    pub(crate) fit: bool,
    pub(crate) parallel: u16,
    pub(crate) http_threads: u16,
    pub(crate) poll: u8,
    pub(crate) batch_poll: u8,
    pub(crate) extra_cache_mib: u32,
    pub(crate) offline: bool,
}

impl ServiceRuntimeProfile {
    pub(crate) fn qualified() -> Self {
        Self {
            max_output_tokens: 4096,
            batch_size: 512,
            micro_batch_size: 128,
            threads: 4,
            batch_threads: 4,
            cache_type_k: "f16".into(),
            cache_type_v: "f16".into(),
            kv_offload: true,
            gpu_layers: "all".into(),
            fit: false,
            parallel: 1,
            http_threads: 1,
            poll: 0,
            batch_poll: 0,
            extra_cache_mib: 0,
            offline: true,
        }
    }
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
    #[serde(skip_serializing_if = "Option::is_none")]
    service_profile: Option<ServiceRuntimeProfile>,
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
            #[serde(default)]
            service_profile: Option<ServiceRuntimeProfile>,
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
            service_profile: wire.service_profile,
        };
        fingerprint.primary.sha256.make_ascii_lowercase();
        if let Some(draft) = &mut fingerprint.draft {
            draft.sha256.make_ascii_lowercase();
        }
        fingerprint
            .validate_recorded()
            .map_err(serde::de::Error::custom)?;
        Ok(fingerprint)
    }
}

impl RuntimeFingerprint {
    fn validate_current(&self) -> Result<(), String> {
        self.validate(false)
    }

    fn validate_recorded(&self) -> Result<(), String> {
        self.validate(true)
    }

    fn validate(&self, allow_legacy_sleep_policy: bool) -> Result<(), String> {
        if !matches!(
            self.schema_version,
            FINGERPRINT_SCHEMA_VERSION | SERVICE_FINGERPRINT_SCHEMA_VERSION
        ) {
            return Err("unsupported runtime fingerprint schema".into());
        }
        crate::paths::validate_id(&self.model_id)?;
        if self.sleep_policy.is_some_and(|sleep_policy| {
            sleep_policy != PERSISTENT_SLEEP_IDLE_SECONDS
                && !(allow_legacy_sleep_policy
                    && sleep_policy == LEGACY_PERSISTENT_SLEEP_IDLE_SECONDS)
        }) {
            return Err("unsupported runtime fingerprint sleep policy".into());
        }
        validate_artifact(&self.primary)?;
        if let Some(draft) = &self.draft {
            validate_artifact(draft)?;
        }
        let profile_is_valid = match (
            self.effective_profile,
            self.draft.is_some(),
            self.sleep_policy,
        ) {
            (EffectiveProfile::Generic, false, _) | (EffectiveProfile::Gemma4Mtp, true, _) => true,
            (EffectiveProfile::PrimaryOnly, false, Some(sleep_policy))
                if sleep_policy == PERSISTENT_SLEEP_IDLE_SECONDS
                    || (allow_legacy_sleep_policy
                        && sleep_policy == LEGACY_PERSISTENT_SLEEP_IDLE_SECONDS) =>
            {
                true
            }
            (EffectiveProfile::PrimaryOnly, false, None)
                if self.schema_version == SERVICE_FINGERPRINT_SCHEMA_VERSION =>
            {
                true
            }
            _ => false,
        };
        if !profile_is_valid {
            return Err("runtime fingerprint profile contradicts its draft artifact".into());
        }
        match (self.schema_version, self.service_profile.as_ref()) {
            (FINGERPRINT_SCHEMA_VERSION, None) => Ok(()),
            (SERVICE_FINGERPRINT_SCHEMA_VERSION, Some(profile))
                if self.sleep_policy.is_none()
                    && *profile == ServiceRuntimeProfile::qualified()
                    && (SERVICE_MIN_CONTEXT..=SERVICE_MAX_CONTEXT)
                        .contains(&self.effective_context) =>
            {
                Ok(())
            }
            (SERVICE_FINGERPRINT_SCHEMA_VERSION, Some(_)) => {
                Err("unsupported service runtime resource profile".into())
            }
            _ => Err("runtime fingerprint schema contradicts its service profile".into()),
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
            service_profile: None,
        };
        fingerprint.validate_current()?;
        Ok(fingerprint)
    }

    pub(crate) fn from_manifest_for_service(
        manifest: &Manifest,
        effective_context: u32,
        effective_profile: EffectiveProfile,
    ) -> Result<Self, String> {
        if effective_profile == EffectiveProfile::PrimaryOnly {
            return Err("primary-only fingerprints must come from the paired fallback".into());
        }
        if !(SERVICE_MIN_CONTEXT..=SERVICE_MAX_CONTEXT).contains(&effective_context) {
            return Err(format!(
                "service context must be between {SERVICE_MIN_CONTEXT} and {SERVICE_MAX_CONTEXT} tokens"
            ));
        }
        let fingerprint = Self {
            schema_version: SERVICE_FINGERPRINT_SCHEMA_VERSION,
            model_id: manifest.id.clone(),
            effective_context,
            effective_profile,
            sleep_policy: None,
            primary: manifest.primary_artifact().into(),
            draft: manifest.draft_artifact().map(Into::into),
            service_profile: Some(ServiceRuntimeProfile::qualified()),
        };
        fingerprint.validate_current()?;
        Ok(fingerprint)
    }

    pub(crate) fn primary_only(&self) -> Option<Self> {
        if self.effective_profile != EffectiveProfile::Gemma4Mtp
            || self.draft.is_none()
            || !self.sleep_policy.is_some_and(is_persistent_sleep_policy)
        {
            return None;
        }
        let mut primary_only = self.clone();
        primary_only.effective_profile = EffectiveProfile::PrimaryOnly;
        primary_only.draft = None;
        Some(primary_only)
    }

    pub(crate) fn primary_only_for_service(&self) -> Option<Self> {
        if self.schema_version != SERVICE_FINGERPRINT_SCHEMA_VERSION
            || self.effective_profile != EffectiveProfile::Gemma4Mtp
            || self.draft.is_none()
            || self.sleep_policy.is_some()
            || self.service_profile.as_ref() != Some(&ServiceRuntimeProfile::qualified())
        {
            return None;
        }
        let mut primary_only = self.clone();
        primary_only.effective_profile = EffectiveProfile::PrimaryOnly;
        primary_only.draft = None;
        primary_only.validate_current().ok()?;
        Some(primary_only)
    }

    pub(crate) fn validate_persistent_lease(&self, model_id: &str) -> Result<(), String> {
        self.validate_current()?;
        if self.model_id != model_id {
            return Err("runtime lease model contradicts its fingerprint".into());
        }
        if self.sleep_policy != Some(PERSISTENT_SLEEP_IDLE_SECONDS) {
            return Err(format!(
                "persistent runtime fingerprint requires sleep policy {PERSISTENT_SLEEP_IDLE_SECONDS}"
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_recorded_persistent_lease(&self, model_id: &str) -> Result<(), String> {
        self.validate_recorded()?;
        if self.model_id != model_id {
            return Err("runtime lease model contradicts its fingerprint".into());
        }
        if !self.sleep_policy.is_some_and(is_persistent_sleep_policy) {
            return Err("persistent runtime fingerprint requires a supported sleep policy".into());
        }
        Ok(())
    }

    pub(crate) fn validate_service_lease(&self, model_id: &str) -> Result<(), String> {
        self.validate_current()?;
        if self.model_id != model_id {
            return Err("runtime lease model contradicts its fingerprint".into());
        }
        if self.sleep_policy.is_some() {
            return Err("service runtime fingerprint must not use an engine sleep policy".into());
        }
        if self.schema_version != SERVICE_FINGERPRINT_SCHEMA_VERSION
            || self.service_profile.as_ref() != Some(&ServiceRuntimeProfile::qualified())
        {
            return Err("service runtime fingerprint is not resource-qualified".into());
        }
        Ok(())
    }

    pub(crate) fn model_id(&self) -> &str {
        &self.model_id
    }

    pub(crate) fn effective_context(&self) -> u32 {
        self.effective_context
    }

    pub(crate) fn effective_profile(&self) -> EffectiveProfile {
        self.effective_profile
    }

    pub(crate) fn primary_local_filename(&self) -> &str {
        &self.primary.local_filename
    }

    pub(crate) fn draft_local_filename(&self) -> Option<&str> {
        self.draft
            .as_ref()
            .map(|artifact| artifact.local_filename.as_str())
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

fn is_persistent_sleep_policy(seconds: u64) -> bool {
    matches!(
        seconds,
        PERSISTENT_SLEEP_IDLE_SECONDS | LEGACY_PERSISTENT_SLEEP_IDLE_SECONDS
    )
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
        primary_only_with_draft["sleep_policy"] = serde_json::json!(60);
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
        primary_only["sleep_policy"] = serde_json::json!(60);
        let mut mtp = generic;
        mtp["effective_profile"] = serde_json::json!("gemma4_mtp");
        mtp["sleep_policy"] = serde_json::json!(60);
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
        wire["sleep_policy"] = serde_json::json!(60);
        wire["primary"]["sha256"] = serde_json::json!("A".repeat(64));
        wire["draft"] = artifact("draft.gguf", &"B".repeat(64), 1);

        let fingerprint = serde_json::from_value::<RuntimeFingerprint>(wire).unwrap();
        let canonical = serde_json::to_value(&fingerprint).unwrap();

        assert_eq!(canonical["effective_context"], 0);
        assert_eq!(canonical["primary"]["sha256"], "a".repeat(64));
        assert_eq!(canonical["draft"]["sha256"], "b".repeat(64));
    }

    #[test]
    fn legacy_sleep_policy_is_decode_and_recovery_only() {
        let mut generic = generic_fingerprint();
        generic["sleep_policy"] = serde_json::json!(60);
        let mut mtp = generic.clone();
        mtp["effective_profile"] = serde_json::json!("gemma4_mtp");
        mtp["draft"] = artifact("draft.gguf", &"b".repeat(64), 1);
        let mut primary_only = generic.clone();
        primary_only["effective_profile"] = serde_json::json!("primary_only");

        for (name, wire, expected_profile) in [
            ("generic", generic, EffectiveProfile::Generic),
            ("MTP", mtp, EffectiveProfile::Gemma4Mtp),
            (
                "primary-only fallback",
                primary_only,
                EffectiveProfile::PrimaryOnly,
            ),
        ] {
            let fingerprint = serde_json::from_value::<RuntimeFingerprint>(wire)
                .unwrap_or_else(|error| panic!("could not decode prior {name} lease: {error}"));

            assert_eq!(fingerprint.effective_profile(), expected_profile);
            assert!(
                fingerprint
                    .validate_recorded_persistent_lease("demo")
                    .is_ok(),
                "prior {name} lease was not valid for recovery"
            );
            assert!(
                fingerprint.validate_persistent_lease("demo").is_err(),
                "prior {name} lease was accepted for current publication or attachment"
            );
        }
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
                "legacy sleep policy outside wire recovery",
                generic.clone(),
                4096,
                EffectiveProfile::Generic,
                Some(60),
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
