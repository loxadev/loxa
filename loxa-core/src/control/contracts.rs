#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_manifest_has_only_wire_dependencies() {
        let manifest = include_str!("../../../loxa-protocol/Cargo.toml");
        assert!(manifest.contains("serde"));
        assert!(manifest.contains("uuid"));
        for forbidden in [
            "axum",
            "tauri",
            "clap",
            "rusqlite",
            "loxa-core",
            "loxa-node",
            "std::fs",
            "std::process",
        ] {
            assert!(
                !manifest.contains(forbidden),
                "forbidden dependency {forbidden}"
            );
        }
    }

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
        assert!(registry::REGISTRY
            .iter()
            .all(|entry| ModelRequest::known(entry.id).is_ok()));
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
pub use loxa_protocol::v1::{
    ControlErrorBody, ControlErrorCode, NodeIdentityProofResponse, NodeStatus,
    CONTROL_PROTOCOL_VERSION,
};
use serde::{Deserialize, Serialize};

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
        registry::REGISTRY
            .iter()
            .find(|entry| entry.id == model_id)
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
