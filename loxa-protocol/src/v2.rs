use crate::identity::parse_uuid_v4;
use crate::ParseIdentityError;
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

macro_rules! uuid_v4_id {
    ($name:ident, $expecting:literal) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(Uuid);

        impl $name {
            #[must_use]
            pub fn new_v4() -> Self {
                Self(Uuid::new_v4())
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "{}", self.0.hyphenated())
            }
        }

        impl FromStr for $name {
            type Err = ParseIdentityError;

            fn from_str(text: &str) -> Result<Self, Self::Err> {
                parse_uuid_v4(text).map(Self)
            }
        }

        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.collect_str(self)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                struct IdVisitor;

                impl Visitor<'_> for IdVisitor {
                    type Value = $name;

                    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                        formatter.write_str($expecting)
                    }

                    fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                        value.parse().map_err(E::custom)
                    }
                }

                deserializer.deserialize_str(IdVisitor)
            }
        }
    };
}

uuid_v4_id!(
    OperationId,
    "a canonical lowercase non-nil UUIDv4 operation ID"
);
uuid_v4_id!(EventId, "a canonical lowercase non-nil UUIDv4 event ID");
uuid_v4_id!(SlotId, "a canonical lowercase non-nil UUIDv4 slot ID");
uuid_v4_id!(
    StreamEpoch,
    "a canonical lowercase non-nil UUIDv4 stream epoch"
);

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct DecimalU64(u64);

impl DecimalU64 {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    #[must_use]
    pub fn checked_next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

impl fmt::Display for DecimalU64 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl Serialize for DecimalU64 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for DecimalU64 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct DecimalVisitor;

        impl Visitor<'_> for DecimalVisitor {
            type Value = DecimalU64;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a canonical unsigned decimal string")
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                if value.is_empty()
                    || (value.len() > 1 && value.starts_with('0'))
                    || !value.bytes().all(|byte| byte.is_ascii_digit())
                {
                    return Err(E::custom("invalid canonical unsigned decimal string"));
                }
                value.parse::<u64>().map(DecimalU64).map_err(E::custom)
            }
        }

        deserializer.deserialize_str(DecimalVisitor)
    }
}

pub const V2_SCHEMA_VERSION: u32 = 2;

fn deserialize_schema_version<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u32, D::Error> {
    let value = u32::deserialize(deserializer)?;
    if value == V2_SCHEMA_VERSION {
        Ok(value)
    } else {
        Err(de::Error::custom("unsupported v2 schema version"))
    }
}

fn deserialize_slot_capacity<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u32, D::Error> {
    let value = u32::deserialize(deserializer)?;
    if value == 1 {
        Ok(value)
    } else {
        Err(de::Error::custom("slot capacity must be one"))
    }
}

fn valid_bounded_text(value: &str, max_bytes: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_bytes
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

fn valid_model_id(value: &str) -> bool {
    valid_bounded_text(value, 256)
}

fn valid_public_message(value: &str) -> bool {
    valid_bounded_text(value, 256)
}

fn valid_control_endpoint(value: &str) -> bool {
    if !value.is_ascii() || value.len() > 256 || value.contains(['@', '?', '#']) {
        return false;
    }
    let Some(authority) = value.strip_prefix("http://") else {
        return false;
    };
    let port = if let Some(port) = authority.strip_prefix("127.0.0.1:") {
        port
    } else if let Some(port) = authority.strip_prefix("localhost:") {
        port
    } else if let Some(port) = authority.strip_prefix("[::1]:") {
        port
    } else {
        return false;
    };
    !port.is_empty()
        && port.bytes().all(|byte| byte.is_ascii_digit())
        && port.parse::<u16>().is_ok_and(|port| port != 0)
}

fn deserialize_model_id<'de, D: Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
    let value = String::deserialize(deserializer)?;
    if valid_model_id(&value) {
        Ok(value)
    } else {
        Err(de::Error::custom("invalid model ID"))
    }
}

fn deserialize_optional_model_id<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<String>, D::Error> {
    let value = Option::<String>::deserialize(deserializer)?;
    if value.as_deref().is_none_or(valid_model_id) {
        Ok(value)
    } else {
        Err(de::Error::custom("invalid model ID"))
    }
}

fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

fn deserialize_public_message<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<String, D::Error> {
    let value = String::deserialize(deserializer)?;
    if valid_public_message(&value) {
        Ok(value)
    } else {
        Err(de::Error::custom("invalid public error message"))
    }
}

fn deserialize_control_endpoint<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<String, D::Error> {
    let value = String::deserialize(deserializer)?;
    if valid_control_endpoint(&value) {
        Ok(value)
    } else {
        Err(de::Error::custom("invalid loopback control endpoint"))
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum V2NodeStatus {
    Running,
    Stopping,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum V2SlotStatus {
    Unloaded,
    Loading,
    Ready,
    Unloading,
    Recovery,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum V2OperationKind {
    Download,
    Load,
    Unload,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum V2OperationStatus {
    Queued,
    Running,
    Cancelling,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum V2EventEntity {
    Node,
    Slot,
    Operation,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum V2SlotErrorCode {
    LifecycleRecoveryRequired,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum V2OperationErrorCode {
    DownloadFailed,
    LoadFailed,
    UnloadFailed,
    NodeRestartedBeforeStart,
    NodeRestarted,
    CancellationOutcomeUnknown,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum V2ControlErrorCode {
    InvalidRequest,
    NodeNotFound,
    SlotNotFound,
    OperationNotFound,
    UnknownModel,
    OperationConflict,
    OperationTerminal,
    CancellationNotSafe,
    ModelUnavailable,
    UnsupportedMediaType,
    NodeStopping,
    StateWriterOverloaded,
    DurableStateUnavailable,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct V2NodeCapabilities {
    pub model_download: bool,
    pub slot_load: bool,
    pub slot_unload: bool,
    pub operation_cancel: bool,
    pub operation_stream: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct V2Node {
    pub node_id: crate::NodeId,
    pub node_instance_id: crate::NodeInstanceId,
    #[serde(deserialize_with = "deserialize_control_endpoint")]
    pub control_endpoint: String,
    pub status: V2NodeStatus,
    #[serde(deserialize_with = "deserialize_slot_capacity")]
    pub slot_capacity: u32,
    pub capabilities: V2NodeCapabilities,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct V2PublicError<C> {
    pub code: C,
    #[serde(deserialize_with = "deserialize_public_message")]
    pub message: String,
}

pub type V2SlotError = V2PublicError<V2SlotErrorCode>;
pub type V2OperationError = V2PublicError<V2OperationErrorCode>;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct V2Slot {
    pub slot_id: SlotId,
    pub node_id: crate::NodeId,
    pub name: String,
    pub status: V2SlotStatus,
    pub model_id: Option<String>,
    pub operation_id: Option<OperationId>,
    pub error: Option<V2SlotError>,
}

impl V2Slot {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.name != "default"
            || self
                .model_id
                .as_deref()
                .is_some_and(|id| !valid_model_id(id))
        {
            return Err("invalid default slot fields");
        }
        let legal = match self.status {
            V2SlotStatus::Unloaded => {
                self.model_id.is_none() && self.operation_id.is_none() && self.error.is_none()
            }
            V2SlotStatus::Loading => self.operation_id.is_some() && self.error.is_none(),
            V2SlotStatus::Ready => {
                self.model_id.is_some() && self.operation_id.is_none() && self.error.is_none()
            }
            V2SlotStatus::Unloading => {
                self.model_id.is_some() && self.operation_id.is_some() && self.error.is_none()
            }
            V2SlotStatus::Recovery => self.operation_id.is_none() && self.error.is_some(),
        };
        legal.then_some(()).ok_or("invalid slot state")
    }
}

impl<'de> Deserialize<'de> for V2Slot {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            slot_id: SlotId,
            node_id: crate::NodeId,
            name: String,
            status: V2SlotStatus,
            #[serde(deserialize_with = "deserialize_optional_model_id")]
            model_id: Option<String>,
            #[serde(deserialize_with = "deserialize_required_option")]
            operation_id: Option<OperationId>,
            #[serde(deserialize_with = "deserialize_required_option")]
            error: Option<V2SlotError>,
        }
        let wire = Wire::deserialize(deserializer)?;
        let value = Self {
            slot_id: wire.slot_id,
            node_id: wire.node_id,
            name: wire.name,
            status: wire.status,
            model_id: wire.model_id,
            operation_id: wire.operation_id,
            error: wire.error,
        };
        value.validate().map_err(de::Error::custom)?;
        Ok(value)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct V2OperationProgress {
    pub completed_bytes: DecimalU64,
    #[serde(deserialize_with = "deserialize_required_option")]
    pub total_bytes: Option<DecimalU64>,
}

impl V2OperationProgress {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self
            .total_bytes
            .is_some_and(|total| self.completed_bytes > total)
        {
            Err("completed bytes exceed total bytes")
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct V2Operation {
    pub operation_id: OperationId,
    pub node_id: crate::NodeId,
    pub kind: V2OperationKind,
    pub status: V2OperationStatus,
    pub slot_id: Option<SlotId>,
    pub model_id: Option<String>,
    pub progress: Option<V2OperationProgress>,
    pub error: Option<V2OperationError>,
    pub created_revision: DecimalU64,
    pub updated_revision: DecimalU64,
    pub created_at_unix_ms: DecimalU64,
    pub updated_at_unix_ms: DecimalU64,
}

impl V2Operation {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self
            .model_id
            .as_deref()
            .is_some_and(|id| !valid_model_id(id))
            || self.created_revision > self.updated_revision
            || self.created_at_unix_ms > self.updated_at_unix_ms
            || self
                .progress
                .as_ref()
                .is_some_and(|progress| progress.validate().is_err())
        {
            return Err("invalid operation fields");
        }
        let kind_valid = match self.kind {
            V2OperationKind::Download => self.slot_id.is_none() && self.model_id.is_some(),
            V2OperationKind::Load => {
                self.slot_id.is_some() && self.model_id.is_some() && self.progress.is_none()
            }
            V2OperationKind::Unload => {
                self.slot_id.is_some() && self.model_id.is_none() && self.progress.is_none()
            }
        };
        let error_valid = matches!(self.status, V2OperationStatus::Failed) == self.error.is_some();
        (kind_valid && error_valid)
            .then_some(())
            .ok_or("invalid operation state")
    }
}

impl<'de> Deserialize<'de> for V2Operation {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            operation_id: OperationId,
            node_id: crate::NodeId,
            kind: V2OperationKind,
            status: V2OperationStatus,
            #[serde(deserialize_with = "deserialize_required_option")]
            slot_id: Option<SlotId>,
            #[serde(deserialize_with = "deserialize_optional_model_id")]
            model_id: Option<String>,
            #[serde(deserialize_with = "deserialize_required_option")]
            progress: Option<V2OperationProgress>,
            #[serde(deserialize_with = "deserialize_required_option")]
            error: Option<V2OperationError>,
            created_revision: DecimalU64,
            updated_revision: DecimalU64,
            created_at_unix_ms: DecimalU64,
            updated_at_unix_ms: DecimalU64,
        }
        let wire = Wire::deserialize(deserializer)?;
        let value = Self {
            operation_id: wire.operation_id,
            node_id: wire.node_id,
            kind: wire.kind,
            status: wire.status,
            slot_id: wire.slot_id,
            model_id: wire.model_id,
            progress: wire.progress,
            error: wire.error,
            created_revision: wire.created_revision,
            updated_revision: wire.updated_revision,
            created_at_unix_ms: wire.created_at_unix_ms,
            updated_at_unix_ms: wire.updated_at_unix_ms,
        };
        value.validate().map_err(de::Error::custom)?;
        Ok(value)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct V2NodeCollection {
    #[serde(deserialize_with = "deserialize_schema_version")]
    pub schema_version: u32,
    pub epoch: StreamEpoch,
    pub revision: DecimalU64,
    pub generated_at_unix_ms: DecimalU64,
    pub nodes: Vec<V2Node>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct V2SlotCollection {
    #[serde(deserialize_with = "deserialize_schema_version")]
    pub schema_version: u32,
    pub epoch: StreamEpoch,
    pub revision: DecimalU64,
    pub generated_at_unix_ms: DecimalU64,
    pub node_id: crate::NodeId,
    pub slots: Vec<V2Slot>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct V2OperationCollection {
    #[serde(deserialize_with = "deserialize_schema_version")]
    pub schema_version: u32,
    pub epoch: StreamEpoch,
    pub revision: DecimalU64,
    pub generated_at_unix_ms: DecimalU64,
    pub operations: Vec<V2Operation>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct V2OperationEnvelope {
    #[serde(deserialize_with = "deserialize_schema_version")]
    pub schema_version: u32,
    pub epoch: StreamEpoch,
    pub revision: DecimalU64,
    pub generated_at_unix_ms: DecimalU64,
    pub operation: V2Operation,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct V2StreamPosition {
    pub epoch: StreamEpoch,
    pub cursor: DecimalU64,
    pub cursor_gap: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct V2ControlEvent {
    #[serde(deserialize_with = "deserialize_schema_version")]
    pub schema_version: u32,
    pub event_id: EventId,
    pub epoch: StreamEpoch,
    pub sequence: DecimalU64,
    pub revision: DecimalU64,
    pub committed_at_unix_ms: DecimalU64,
    pub entity: V2EventEntity,
    pub entity_id: String,
    pub node_id: crate::NodeId,
    pub node_instance_id: Option<crate::NodeInstanceId>,
    pub slot_id: Option<SlotId>,
    pub operation_id: Option<OperationId>,
    pub node: Option<V2Node>,
    pub slot: Option<V2Slot>,
    pub operation: Option<V2Operation>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct V2ControlEventWire {
    #[serde(deserialize_with = "deserialize_schema_version")]
    schema_version: u32,
    event_id: EventId,
    epoch: StreamEpoch,
    sequence: DecimalU64,
    revision: DecimalU64,
    committed_at_unix_ms: DecimalU64,
    entity: V2EventEntity,
    entity_id: String,
    node_id: crate::NodeId,
    #[serde(deserialize_with = "deserialize_required_option")]
    node_instance_id: Option<crate::NodeInstanceId>,
    #[serde(deserialize_with = "deserialize_required_option")]
    slot_id: Option<SlotId>,
    #[serde(deserialize_with = "deserialize_required_option")]
    operation_id: Option<OperationId>,
    #[serde(deserialize_with = "deserialize_required_option")]
    node: Option<V2Node>,
    #[serde(deserialize_with = "deserialize_required_option")]
    slot: Option<V2Slot>,
    #[serde(deserialize_with = "deserialize_required_option")]
    operation: Option<V2Operation>,
}

impl V2ControlEvent {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.node.is_none() && self.slot.is_none() && self.operation.is_none() {
            return Err("event has no committed record");
        }
        if self
            .node
            .as_ref()
            .is_some_and(|node| node.node_id != self.node_id)
            || self
                .slot
                .as_ref()
                .is_some_and(|slot| slot.node_id != self.node_id)
            || self
                .operation
                .as_ref()
                .is_some_and(|operation| operation.node_id != self.node_id)
            || self
                .slot
                .as_ref()
                .is_some_and(|slot| Some(slot.slot_id) != self.slot_id)
            || self
                .operation
                .as_ref()
                .is_some_and(|operation| Some(operation.operation_id) != self.operation_id)
        {
            return Err("event correlation mismatch");
        }
        let entity_matches = match self.entity {
            V2EventEntity::Node => {
                self.node_id.to_string() == self.entity_id && self.node.is_some()
            }
            V2EventEntity::Slot => {
                self.slot_id
                    .is_some_and(|id| id.to_string() == self.entity_id)
                    && self.slot.is_some()
            }
            V2EventEntity::Operation => {
                self.operation_id
                    .is_some_and(|id| id.to_string() == self.entity_id)
                    && self.operation.is_some()
            }
        };
        entity_matches.then_some(()).ok_or("event entity mismatch")
    }
}

impl<'de> Deserialize<'de> for V2ControlEvent {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = V2ControlEventWire::deserialize(deserializer)?;
        let value = Self {
            schema_version: wire.schema_version,
            event_id: wire.event_id,
            epoch: wire.epoch,
            sequence: wire.sequence,
            revision: wire.revision,
            committed_at_unix_ms: wire.committed_at_unix_ms,
            entity: wire.entity,
            entity_id: wire.entity_id,
            node_id: wire.node_id,
            node_instance_id: wire.node_instance_id,
            slot_id: wire.slot_id,
            operation_id: wire.operation_id,
            node: wire.node,
            slot: wire.slot,
            operation: wire.operation,
        };
        value.validate().map_err(de::Error::custom)?;
        Ok(value)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct V2ReconnectSnapshot {
    #[serde(deserialize_with = "deserialize_schema_version")]
    pub schema_version: u32,
    pub epoch: StreamEpoch,
    pub revision: DecimalU64,
    pub generated_at_unix_ms: DecimalU64,
    pub stream: V2StreamPosition,
    pub nodes: Vec<V2Node>,
    pub slots: Vec<V2Slot>,
    pub operations: Vec<V2Operation>,
    pub events: Vec<V2ControlEvent>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct V2OperationAccepted {
    pub epoch: StreamEpoch,
    pub operation_id: OperationId,
    pub revision: DecimalU64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct V2ControlErrorBody {
    pub code: V2ControlErrorCode,
    #[serde(deserialize_with = "deserialize_public_message")]
    pub message: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct V2LoadRequest {
    #[serde(deserialize_with = "deserialize_model_id")]
    pub model_id: String,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct V2EmptyRequest {}
