#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_model_requests_reject_unknown_or_blank_ids() {
        assert!(ModelRequest::known("gemma-3-4b-it-q4").is_ok());
        assert_eq!(
            ModelRequest::known("unknown").unwrap_err(),
            ContractError::UnknownModel
        );
        assert_eq!(
            ModelRequest::known(" ").unwrap_err(),
            ContractError::UnknownModel
        );
        assert!(serde_json::from_str::<ModelRequest>(r#"{"model_id":"unknown"}"#).is_err());
    }

    #[test]
    fn schemas_are_closed_and_use_stable_snake_case_states() {
        let json = serde_json::to_string(&OperationStatus::Failed).unwrap();
        assert_eq!(json, "\"failed\"");
        assert_eq!(
            serde_json::to_string(&NodeStatus::RecoveryRequired).unwrap(),
            "\"recovery_required\""
        );
        assert!(serde_json::from_str::<OperationStatus>("\"recovery_required\"").is_err());
        assert!(serde_json::from_str::<ModelRequest>(
            r#"{"model_id":"gemma-3-4b-it-q4","extra":true}"#
        )
        .is_err());
    }

    #[test]
    fn node_identity_challenge_and_response_are_closed_and_strictly_typed() {
        let nonce = "01".repeat(32);
        assert!(serde_json::from_value::<NodeIdentityChallenge>(
            serde_json::json!({"nonce": nonce})
        )
        .is_ok());
        for invalid in [
            serde_json::json!({}),
            serde_json::json!({"nonce": "01", "extra": true}),
            serde_json::json!({"nonce": 7}),
            serde_json::json!({"nonce": "AA".repeat(32)}),
        ] {
            assert!(serde_json::from_value::<NodeIdentityChallenge>(invalid).is_err());
        }
        let valid = serde_json::json!({
            "protocol_version": 1, "node_id": "node", "runtime_identity": "runtime",
            "status": "unloaded", "challenge_proof": "00".repeat(32),
        });
        assert!(serde_json::from_value::<NodeIdentityProofResponse>(valid.clone()).is_ok());
        let mut extra = valid.clone();
        extra
            .as_object_mut()
            .unwrap()
            .insert("extra".into(), true.into());
        assert!(serde_json::from_value::<NodeIdentityProofResponse>(extra).is_err());
        for field in [
            "protocol_version",
            "node_id",
            "runtime_identity",
            "status",
            "challenge_proof",
        ] {
            let mut missing = valid.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(
                serde_json::from_value::<NodeIdentityProofResponse>(missing).is_err(),
                "{field}"
            );
        }
        for invalid in [
            serde_json::json!({"protocol_version": 2, "node_id": "node", "runtime_identity": "runtime", "status": "unloaded", "challenge_proof": "00".repeat(32)}),
            serde_json::json!({"protocol_version": 1, "node_id": "", "runtime_identity": "runtime", "status": "unloaded", "challenge_proof": "00".repeat(32)}),
            serde_json::json!({"protocol_version": 1, "node_id": "n".repeat(1025), "runtime_identity": "runtime", "status": "unloaded", "challenge_proof": "00".repeat(32)}),
            serde_json::json!({"protocol_version": 1, "node_id": "node", "runtime_identity": "", "status": "unloaded", "challenge_proof": "00".repeat(32)}),
            serde_json::json!({"protocol_version": 1, "node_id": "node", "runtime_identity": "runtime", "status": "unloaded", "challenge_proof": "AA".repeat(32)}),
            serde_json::json!({"protocol_version": 1, "node_id": "node", "runtime_identity": "runtime", "status": "unloaded", "challenge_proof": "00"}),
        ] {
            assert!(serde_json::from_value::<NodeIdentityProofResponse>(invalid).is_err());
        }
    }
}
use crate::registry;
use serde::{Deserialize, Serialize};

pub const CONTROL_PROTOCOL_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ContractError {
    UnknownModel,
    InvalidChallenge,
    InvalidProofResponse,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ModelRequest {
    pub model_id: String,
}

impl<'de> Deserialize<'de> for ModelRequest {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WireRequest {
            model_id: String,
        }
        let request = WireRequest::deserialize(deserializer)?;
        Self::known(&request.model_id).map_err(|_| serde::de::Error::custom("unknown model id"))
    }
}

impl ModelRequest {
    pub fn known(model_id: &str) -> Result<Self, ContractError> {
        registry::find(model_id)
            .map(|_| Self {
                model_id: model_id.to_owned(),
            })
            .ok_or(ContractError::UnknownModel)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationKind {
    Download,
    Load,
    Unload,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OperationProgress {
    pub completed_bytes: u64,
    pub total_bytes: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OperationView {
    pub id: String,
    pub kind: OperationKind,
    pub status: OperationStatus,
    pub model_id: Option<String>,
    pub progress: Option<OperationProgress>,
    pub error: Option<String>,
    pub created_at_unix_ms: u64,
    pub updated_at_unix_ms: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ControlEvent {
    pub sequence: u64,
    pub operation: OperationView,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReconnectSnapshot {
    pub cursor: u64,
    pub cursor_gap: bool,
    pub operations: Vec<OperationView>,
    pub events: Vec<ControlEvent>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilitiesSnapshot {
    pub document_input: bool,
    pub document_input_reason: String,
    pub text_chat: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    Unloaded,
    Loading,
    Ready,
    Unloading,
    RecoveryRequired,
    Error,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct NodeIdentityChallenge {
    pub nonce: String,
}

impl NodeIdentityChallenge {
    pub fn new(nonce: &str) -> Result<Self, ContractError> {
        if nonce.len() == 64
            && nonce
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            Ok(Self {
                nonce: nonce.to_owned(),
            })
        } else {
            Err(ContractError::InvalidChallenge)
        }
    }
}

impl<'de> Deserialize<'de> for NodeIdentityChallenge {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WireChallenge {
            nonce: String,
        }
        let wire = WireChallenge::deserialize(deserializer)?;
        Self::new(&wire.nonce)
            .map_err(|_| serde::de::Error::custom("invalid node identity challenge"))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct NodeIdentityProofResponse {
    pub protocol_version: u32,
    pub node_id: String,
    pub runtime_identity: String,
    pub status: NodeStatus,
    pub challenge_proof: String,
}

impl NodeIdentityProofResponse {
    pub fn new(
        protocol_version: u32,
        node_id: String,
        runtime_identity: String,
        status: NodeStatus,
        challenge_proof: String,
    ) -> Result<Self, ContractError> {
        let identity_valid = |value: &str| !value.is_empty() && value.len() <= 1024;
        let proof_valid = challenge_proof.len() == 64
            && challenge_proof
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase());
        if protocol_version != CONTROL_PROTOCOL_VERSION
            || !identity_valid(&node_id)
            || !identity_valid(&runtime_identity)
            || !proof_valid
        {
            return Err(ContractError::InvalidProofResponse);
        }
        Ok(Self {
            protocol_version,
            node_id,
            runtime_identity,
            status,
            challenge_proof,
        })
    }
}

impl<'de> Deserialize<'de> for NodeIdentityProofResponse {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WireResponse {
            protocol_version: u32,
            node_id: String,
            runtime_identity: String,
            status: NodeStatus,
            challenge_proof: String,
        }
        let wire = WireResponse::deserialize(deserializer)?;
        Self::new(
            wire.protocol_version,
            wire.node_id,
            wire.runtime_identity,
            wire.status,
            wire.challenge_proof,
        )
        .map_err(|_| serde::de::Error::custom("invalid node identity proof response"))
    }
}

impl NodeStatus {
    pub(crate) fn proof_discriminant(self) -> u8 {
        match self {
            Self::Unloaded => 0,
            Self::Loading => 1,
            Self::Ready => 2,
            Self::Unloading => 3,
            Self::RecoveryRequired => 4,
            Self::Error => 5,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NodeSnapshot {
    pub status: NodeStatus,
    pub active_model_id: Option<String>,
    pub operation_id: Option<String>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OperationAccepted {
    pub operation_id: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ControlErrorBody {
    pub code: String,
    pub message: String,
}
