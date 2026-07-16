use serde::{Deserialize, Serialize};
use std::fmt;

pub const CONTROL_PROTOCOL_VERSION: u32 = 1;

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

impl NodeStatus {
    #[doc(hidden)]
    #[must_use]
    pub const fn proof_discriminant(self) -> u8 {
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InvalidProofResponse;

impl fmt::Display for InvalidProofResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid node identity proof response")
    }
}

impl std::error::Error for InvalidProofResponse {}

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
    ) -> Result<Self, InvalidProofResponse> {
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
            return Err(InvalidProofResponse);
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
        .map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlErrorCode {
    OperationConflict,
    OperationNotFound,
    OperationTerminal,
    NodeStopping,
    CancellationNotSafe,
    ModelUnavailable,
    UnsupportedMediaType,
    UnknownModel,
}

impl fmt::Display for ControlErrorCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::OperationConflict => "operation_conflict",
            Self::OperationNotFound => "operation_not_found",
            Self::OperationTerminal => "operation_terminal",
            Self::NodeStopping => "node_stopping",
            Self::CancellationNotSafe => "cancellation_not_safe",
            Self::ModelUnavailable => "model_unavailable",
            Self::UnsupportedMediaType => "unsupported_media_type",
            Self::UnknownModel => "unknown_model",
        })
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ControlErrorBody {
    pub code: String,
    pub message: String,
}
