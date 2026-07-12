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
        assert!(
            serde_json::from_str::<ModelRequest>(r#"{"model_id":"gemma-3-4b-it-q4","extra":true}"#)
                .is_err()
        );
    }
}
use crate::registry;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ContractError {
    UnknownModel,
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
