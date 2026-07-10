use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderKind {
    ManagedLlama,
    Ollama,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SamplingPolicy {
    pub temperature_milli: u32,
    pub top_p_milli: u32,
    pub seed: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CandidateIdentity {
    pub candidate_id: String,
    pub provider: ProviderKind,
    pub provider_version: String,
    pub engine_revision: Option<String>,
    pub model_id: String,
    pub artifact_digest: String,
    pub tokenizer_digest: String,
    pub chat_template_digest: String,
    pub context_tokens: u32,
    pub required_free_memory_bytes: u64,
    pub sampling: SamplingPolicy,
}

impl CandidateIdentity {
    pub fn identity_errors(&self) -> Vec<String> {
        let mut errors = Vec::new();

        if self.candidate_id.trim().is_empty() {
            errors.push("candidate_id".to_string());
        }
        if self.provider_version.trim().is_empty() {
            errors.push("provider_version".to_string());
        }
        if self
            .engine_revision
            .as_deref()
            .is_none_or(|revision| revision.trim().is_empty())
        {
            errors.push("engine_revision".to_string());
        }
        if self.model_id.trim().is_empty() {
            errors.push("model_id".to_string());
        }
        if self.artifact_digest.trim().is_empty() {
            errors.push("artifact_digest".to_string());
        }
        if self.tokenizer_digest.trim().is_empty() {
            errors.push("tokenizer_digest".to_string());
        }
        if self.chat_template_digest.trim().is_empty() {
            errors.push("chat_template_digest".to_string());
        }
        if self.context_tokens == 0 {
            errors.push("context_tokens".to_string());
        }
        if self.required_free_memory_bytes == 0 {
            errors.push("required_free_memory_bytes".to_string());
        }

        errors
    }
}

#[cfg(test)]
mod tests {
    use super::{CandidateIdentity, ProviderKind, SamplingPolicy};

    fn complete_identity() -> CandidateIdentity {
        CandidateIdentity {
            candidate_id: "managed-gemma3-4b".into(),
            provider: ProviderKind::ManagedLlama,
            provider_version: "9910-f5525f7e7".into(),
            engine_revision: Some("f5525f7e7".into()),
            model_id: "gemma-3-4b-it-q4".into(),
            artifact_digest:
                "sha256:04a43a22e8d2003deda5acc262f68ec1005fa76c735a9962a8c77042a74a7d19".into(),
            tokenizer_digest: "sha256:test-tokenizer".into(),
            chat_template_digest: "sha256:test-template".into(),
            context_tokens: 8192,
            required_free_memory_bytes: 2_863_378_119,
            sampling: SamplingPolicy {
                temperature_milli: 0,
                top_p_milli: 1000,
                seed: 1,
            },
        }
    }

    #[test]
    fn complete_identity_has_stable_json_round_trip() {
        let identity = complete_identity();

        assert!(identity.identity_errors().is_empty());
        assert_eq!(
            serde_json::from_str::<CandidateIdentity>(
                &serde_json::to_string(&identity).expect("serialize candidate identity")
            )
            .expect("deserialize candidate identity"),
            identity
        );
    }

    #[test]
    fn incomplete_identity_reports_stable_field_names() {
        let empty_identity = CandidateIdentity {
            candidate_id: String::new(),
            provider_version: String::new(),
            engine_revision: None,
            model_id: String::new(),
            artifact_digest: String::new(),
            tokenizer_digest: String::new(),
            chat_template_digest: String::new(),
            context_tokens: 0,
            required_free_memory_bytes: 0,
            ..complete_identity()
        };
        let whitespace_identity = CandidateIdentity {
            candidate_id: " \t".into(),
            provider_version: " \n".into(),
            engine_revision: Some(" \t".into()),
            model_id: " ".into(),
            artifact_digest: " \r".into(),
            tokenizer_digest: "\n".into(),
            chat_template_digest: "\t".into(),
            ..complete_identity()
        };
        let all_field_errors = vec![
            "candidate_id".to_string(),
            "provider_version".to_string(),
            "engine_revision".to_string(),
            "model_id".to_string(),
            "artifact_digest".to_string(),
            "tokenizer_digest".to_string(),
            "chat_template_digest".to_string(),
            "context_tokens".to_string(),
            "required_free_memory_bytes".to_string(),
        ];

        assert_eq!(empty_identity.identity_errors(), all_field_errors);
        assert_eq!(
            whitespace_identity.identity_errors(),
            vec![
                "candidate_id".to_string(),
                "provider_version".to_string(),
                "engine_revision".to_string(),
                "model_id".to_string(),
                "artifact_digest".to_string(),
                "tokenizer_digest".to_string(),
                "chat_template_digest".to_string(),
            ]
        );
    }

    #[test]
    fn provider_kind_serializes_with_stable_names() {
        assert_eq!(
            serde_json::to_string(&ProviderKind::ManagedLlama).unwrap(),
            r#""managed_llama""#
        );
        assert_eq!(
            serde_json::to_string(&ProviderKind::Ollama).unwrap(),
            r#""ollama""#
        );
    }
}
