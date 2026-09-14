use super::validation::validate_prepared;
use super::{invalid, limit};
use crate::history::HistoryError;
use crate::runtime_fingerprint::RuntimeFingerprint;
use crate::runtime_identity::RuntimeIdentity;
use serde::Serialize;
use std::sync::Arc;

pub(super) const MAX_USER_TEXT_BYTES: usize = 32 * 1024;
pub(super) const MAX_SYSTEM_TEXT_BYTES: usize = 16 * 1024;
const MAX_PROMPT_BASIS_BYTES: usize = 128 * 1024;
const MAX_PROMPT_REFERENCES: usize = 512;
const MAX_ADMISSION_BACKING_BYTES: usize = 256 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct PromptReference {
    pub(crate) turn_id: [u8; 16],
    pub(crate) attempt_id: Option<[u8; 16]>,
    pub(crate) prefix_end: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PromptBasis {
    pub(crate) references: Vec<PromptReference>,
}

impl PromptBasis {
    fn encode(&self) -> Result<Vec<u8>, HistoryError> {
        if self.references.len() > MAX_PROMPT_REFERENCES {
            return Err(invalid("prompt basis contains too many references"));
        }
        let encoded = serde_json::to_vec(&self.references)
            .map_err(|_| invalid("prompt basis could not be encoded"))?;
        if encoded.len() > MAX_PROMPT_BASIS_BYTES {
            return Err(invalid("prompt basis exceeds 128 KiB"));
        }
        Ok(encoded)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DraftSubmission {
    pub(crate) id: [u8; 16],
    pub(crate) desktop_client_id: [u8; 16],
    pub(crate) revision: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AdmissionKind {
    #[cfg_attr(not(test), allow(dead_code))]
    Send {
        user_text: String,
        draft: Option<DraftSubmission>,
    },
    #[cfg_attr(not(test), allow(dead_code))]
    Retry { prior_attempt_id: [u8; 16] },
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedAdmission {
    pub(super) conversation_id: [u8; 16],
    pub(super) submission_id: [u8; 16],
    pub(super) submission_hash: [u8; 32],
    pub(super) expected_conversation_revision: i64,
    pub(super) expected_profile_revision: i64,
    pub(super) owner_epoch: String,
    pub(super) operation_generation: i64,
    pub(super) runtime_fingerprint: Arc<RuntimeFingerprint>,
    pub(super) runtime_identity: RuntimeIdentity,
    pub(super) effective_context: u32,
    pub(super) system_instruction: String,
    pub(super) max_output_tokens: i64,
    pub(super) prompt_basis: PromptBasis,
    pub(super) kind: AdmissionKind,
    pub(super) runtime_fingerprint_json: Vec<u8>,
    pub(super) prompt_basis_json: Vec<u8>,
}

impl PreparedAdmission {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        conversation_id: [u8; 16],
        submission_id: [u8; 16],
        submission_hash: [u8; 32],
        expected_conversation_revision: i64,
        expected_profile_revision: i64,
        owner_epoch: String,
        operation_generation: i64,
        runtime_fingerprint: Arc<RuntimeFingerprint>,
        runtime_identity: RuntimeIdentity,
        effective_context: u32,
        system_instruction: String,
        max_output_tokens: i64,
        prompt_basis: PromptBasis,
        kind: AdmissionKind,
    ) -> Result<Self, HistoryError> {
        let mut prepared = Self {
            conversation_id,
            submission_id,
            submission_hash,
            expected_conversation_revision,
            expected_profile_revision,
            owner_epoch,
            operation_generation,
            runtime_fingerprint,
            runtime_identity,
            effective_context,
            system_instruction,
            max_output_tokens,
            prompt_basis,
            kind,
            runtime_fingerprint_json: Vec::new(),
            prompt_basis_json: Vec::new(),
        };
        validate_prepared(&prepared)?;
        if prepared.retained_backing_bytes() > MAX_ADMISSION_BACKING_BYTES {
            return Err(limit("history admission backing exceeds 256 KiB"));
        }
        prepared.runtime_fingerprint_json = serde_json::to_vec(&*prepared.runtime_fingerprint)
            .map_err(|_| invalid("runtime fingerprint could not be encoded"))?;
        if prepared.runtime_fingerprint_json.len() > MAX_PROMPT_BASIS_BYTES {
            return Err(invalid("runtime fingerprint exceeds 128 KiB"));
        }
        prepared.prompt_basis_json = prepared.prompt_basis.encode()?;
        if prepared.retained_backing_bytes() > MAX_ADMISSION_BACKING_BYTES {
            return Err(limit("history admission backing exceeds 256 KiB"));
        }
        Ok(prepared)
    }

    pub(crate) fn submission_id(&self) -> [u8; 16] {
        self.submission_id
    }

    pub(crate) fn submission_hash(&self) -> [u8; 32] {
        self.submission_hash
    }

    pub(crate) fn kind(&self) -> &AdmissionKind {
        &self.kind
    }

    fn retained_backing_bytes(&self) -> usize {
        let kind = match &self.kind {
            AdmissionKind::Send { user_text, .. } => user_text.capacity(),
            AdmissionKind::Retry { .. } => 0,
        };
        std::mem::size_of::<Self>()
            .saturating_add(std::mem::size_of::<RuntimeFingerprint>())
            .saturating_add(2 * std::mem::size_of::<usize>())
            .saturating_add(std::mem::align_of::<RuntimeFingerprint>().saturating_sub(1))
            .saturating_add(self.owner_epoch.capacity())
            .saturating_add(self.runtime_fingerprint.retained_backing_bytes())
            .saturating_add(self.system_instruction.capacity())
            .saturating_add(
                self.prompt_basis
                    .references
                    .capacity()
                    .saturating_mul(std::mem::size_of::<PromptReference>()),
            )
            .saturating_add(kind)
            .saturating_add(self.runtime_fingerprint_json.capacity())
            .saturating_add(self.prompt_basis_json.capacity())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommittedAdmission {
    pub(crate) conversation_id: [u8; 16],
    pub(crate) turn_id: [u8; 16],
    pub(crate) attempt_id: [u8; 16],
    pub(crate) submission_id: [u8; 16],
    pub(crate) pre_conversation_revision: i64,
    pub(crate) post_conversation_revision: i64,
    pub(crate) profile_revision: i64,
    pub(crate) owner_epoch: String,
    pub(crate) operation_generation: i64,
}
