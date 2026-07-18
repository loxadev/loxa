use crate::identity::parse_uuid_v4;
use crate::ParseIdentityError;
use serde::de::{self, Visitor};
use serde::ser::{Error as _, SerializeStruct};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashSet;
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

fn deserialize_nonzero_decimal_u64<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<DecimalU64, D::Error> {
    let value = DecimalU64::deserialize(deserializer)?;
    if value.get() == 0 {
        Err(de::Error::custom("committed counter must be nonzero"))
    } else {
        Ok(value)
    }
}

fn serialize_nonzero_decimal_u64<S: Serializer>(
    value: &DecimalU64,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    if value.get() == 0 {
        Err(S::Error::custom("committed counter must be nonzero"))
    } else {
        value.serialize(serializer)
    }
}

pub const V2_SCHEMA_VERSION: u32 = 2;
const MAX_PUBLIC_OPERATIONS: usize = 256;

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

fn serialize_slot_capacity<S: Serializer>(value: &u32, serializer: S) -> Result<S::Ok, S::Error> {
    if *value != 1 {
        return Err(S::Error::custom("slot capacity must be one"));
    }
    serializer.serialize_u32(*value)
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

fn serialize_model_id<S: Serializer>(value: &str, serializer: S) -> Result<S::Ok, S::Error> {
    if !valid_model_id(value) {
        return Err(S::Error::custom("invalid model ID"));
    }
    serializer.serialize_str(value)
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

fn serialize_public_message<S: Serializer>(value: &str, serializer: S) -> Result<S::Ok, S::Error> {
    if !valid_public_message(value) {
        return Err(S::Error::custom("invalid public error message"));
    }
    serializer.serialize_str(value)
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

fn serialize_control_endpoint<S: Serializer>(
    value: &str,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    if !valid_control_endpoint(value) {
        return Err(S::Error::custom("invalid loopback control endpoint"));
    }
    serializer.serialize_str(value)
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
    #[serde(
        deserialize_with = "deserialize_control_endpoint",
        serialize_with = "serialize_control_endpoint"
    )]
    pub control_endpoint: String,
    pub status: V2NodeStatus,
    #[serde(
        deserialize_with = "deserialize_slot_capacity",
        serialize_with = "serialize_slot_capacity"
    )]
    pub slot_capacity: u32,
    pub capabilities: V2NodeCapabilities,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct V2PublicError<C> {
    pub code: C,
    #[serde(
        deserialize_with = "deserialize_public_message",
        serialize_with = "serialize_public_message"
    )]
    pub message: String,
}

pub type V2SlotError = V2PublicError<V2SlotErrorCode>;
pub type V2OperationError = V2PublicError<V2OperationErrorCode>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V2Slot {
    pub slot_id: SlotId,
    pub node_id: crate::NodeId,
    pub name: String,
    pub status: V2SlotStatus,
    pub model_id: Option<String>,
    pub operation_id: Option<OperationId>,
    pub error: Option<V2SlotError>,
}

impl Serialize for V2Slot {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(S::Error::custom)?;
        let mut state = serializer.serialize_struct("V2Slot", 7)?;
        state.serialize_field("slot_id", &self.slot_id)?;
        state.serialize_field("node_id", &self.node_id)?;
        state.serialize_field("name", &self.name)?;
        state.serialize_field("status", &self.status)?;
        state.serialize_field("model_id", &self.model_id)?;
        state.serialize_field("operation_id", &self.operation_id)?;
        state.serialize_field("error", &self.error)?;
        state.end()
    }
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V2OperationProgress {
    pub completed_bytes: DecimalU64,
    pub total_bytes: Option<DecimalU64>,
}

impl Serialize for V2OperationProgress {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(S::Error::custom)?;
        let mut state = serializer.serialize_struct("V2OperationProgress", 2)?;
        state.serialize_field("completed_bytes", &self.completed_bytes)?;
        state.serialize_field("total_bytes", &self.total_bytes)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for V2OperationProgress {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            completed_bytes: DecimalU64,
            #[serde(deserialize_with = "deserialize_required_option")]
            total_bytes: Option<DecimalU64>,
        }
        let wire = Wire::deserialize(deserializer)?;
        let value = Self {
            completed_bytes: wire.completed_bytes,
            total_bytes: wire.total_bytes,
        };
        value.validate().map_err(de::Error::custom)?;
        Ok(value)
    }
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

#[derive(Clone, Debug, Eq, PartialEq)]
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
            || self.created_revision.get() == 0
            || self.updated_revision.get() == 0
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
        let error_valid = matches!(self.status, V2OperationStatus::Failed) == self.error.is_some()
            && self.error.as_ref().is_none_or(|error| match error.code {
                V2OperationErrorCode::DownloadFailed => self.kind == V2OperationKind::Download,
                V2OperationErrorCode::LoadFailed => self.kind == V2OperationKind::Load,
                V2OperationErrorCode::UnloadFailed => self.kind == V2OperationKind::Unload,
                V2OperationErrorCode::NodeRestartedBeforeStart
                | V2OperationErrorCode::NodeRestarted
                | V2OperationErrorCode::CancellationOutcomeUnknown => true,
            });
        (kind_valid && error_valid)
            .then_some(())
            .ok_or("invalid operation state")
    }
}

impl Serialize for V2Operation {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(S::Error::custom)?;
        let mut state = serializer.serialize_struct("V2Operation", 12)?;
        state.serialize_field("operation_id", &self.operation_id)?;
        state.serialize_field("node_id", &self.node_id)?;
        state.serialize_field("kind", &self.kind)?;
        state.serialize_field("status", &self.status)?;
        state.serialize_field("slot_id", &self.slot_id)?;
        state.serialize_field("model_id", &self.model_id)?;
        state.serialize_field("progress", &self.progress)?;
        state.serialize_field("error", &self.error)?;
        state.serialize_field("created_revision", &self.created_revision)?;
        state.serialize_field("updated_revision", &self.updated_revision)?;
        state.serialize_field("created_at_unix_ms", &self.created_at_unix_ms)?;
        state.serialize_field("updated_at_unix_ms", &self.updated_at_unix_ms)?;
        state.end()
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V2NodeCollection {
    pub schema_version: u32,
    pub epoch: StreamEpoch,
    pub revision: DecimalU64,
    pub generated_at_unix_ms: DecimalU64,
    pub nodes: Vec<V2Node>,
}

impl V2NodeCollection {
    pub fn validate(&self) -> Result<(), &'static str> {
        (self.schema_version == V2_SCHEMA_VERSION
            && self.revision.get() != 0
            && self.nodes.len() == 1)
            .then_some(())
            .ok_or("node collection must contain exactly one v2 node")
    }
}

impl Serialize for V2NodeCollection {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(S::Error::custom)?;
        let mut state = serializer.serialize_struct("V2NodeCollection", 5)?;
        state.serialize_field("schema_version", &self.schema_version)?;
        state.serialize_field("epoch", &self.epoch)?;
        state.serialize_field("revision", &self.revision)?;
        state.serialize_field("generated_at_unix_ms", &self.generated_at_unix_ms)?;
        state.serialize_field("nodes", &self.nodes)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for V2NodeCollection {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            #[serde(deserialize_with = "deserialize_schema_version")]
            schema_version: u32,
            epoch: StreamEpoch,
            revision: DecimalU64,
            generated_at_unix_ms: DecimalU64,
            nodes: Vec<V2Node>,
        }
        let wire = Wire::deserialize(deserializer)?;
        let value = Self {
            schema_version: wire.schema_version,
            epoch: wire.epoch,
            revision: wire.revision,
            generated_at_unix_ms: wire.generated_at_unix_ms,
            nodes: wire.nodes,
        };
        value.validate().map_err(de::Error::custom)?;
        Ok(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V2SlotCollection {
    pub schema_version: u32,
    pub epoch: StreamEpoch,
    pub revision: DecimalU64,
    pub generated_at_unix_ms: DecimalU64,
    pub node_id: crate::NodeId,
    pub slots: Vec<V2Slot>,
}

impl V2SlotCollection {
    pub fn validate(&self) -> Result<(), &'static str> {
        (self.schema_version == V2_SCHEMA_VERSION
            && self.revision.get() != 0
            && self.slots.len() == 1
            && self.slots[0].node_id == self.node_id)
            .then_some(())
            .ok_or("slot collection must contain the correlated default slot")
    }
}

impl Serialize for V2SlotCollection {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(S::Error::custom)?;
        let mut state = serializer.serialize_struct("V2SlotCollection", 6)?;
        state.serialize_field("schema_version", &self.schema_version)?;
        state.serialize_field("epoch", &self.epoch)?;
        state.serialize_field("revision", &self.revision)?;
        state.serialize_field("generated_at_unix_ms", &self.generated_at_unix_ms)?;
        state.serialize_field("node_id", &self.node_id)?;
        state.serialize_field("slots", &self.slots)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for V2SlotCollection {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            #[serde(deserialize_with = "deserialize_schema_version")]
            schema_version: u32,
            epoch: StreamEpoch,
            revision: DecimalU64,
            generated_at_unix_ms: DecimalU64,
            node_id: crate::NodeId,
            slots: Vec<V2Slot>,
        }
        let wire = Wire::deserialize(deserializer)?;
        let value = Self {
            schema_version: wire.schema_version,
            epoch: wire.epoch,
            revision: wire.revision,
            generated_at_unix_ms: wire.generated_at_unix_ms,
            node_id: wire.node_id,
            slots: wire.slots,
        };
        value.validate().map_err(de::Error::custom)?;
        Ok(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V2OperationCollection {
    pub schema_version: u32,
    pub epoch: StreamEpoch,
    pub revision: DecimalU64,
    pub generated_at_unix_ms: DecimalU64,
    pub operations: Vec<V2Operation>,
}

impl V2OperationCollection {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.schema_version != V2_SCHEMA_VERSION || self.revision.get() == 0 {
            return Err("unsupported v2 schema version");
        }
        if self.operations.len() > MAX_PUBLIC_OPERATIONS {
            return Err("operation collection exceeds public retention bound");
        }
        let mut operation_ids = HashSet::with_capacity(self.operations.len());
        let node_id = self.operations.first().map(|operation| operation.node_id);
        for operation in &self.operations {
            if node_id.is_some_and(|node_id| node_id != operation.node_id)
                || operation.updated_revision > self.revision
                || operation.updated_at_unix_ms > self.generated_at_unix_ms
                || !operation_ids.insert(operation.operation_id)
            {
                return Err("operation collection correlation mismatch");
            }
        }
        if !operations_are_canonical(&self.operations) {
            return Err("operation collection is not canonically ordered");
        }
        Ok(())
    }
}

fn operations_are_canonical(operations: &[V2Operation]) -> bool {
    operations.windows(2).all(|pair| {
        (pair[0].created_revision, pair[0].operation_id)
            < (pair[1].created_revision, pair[1].operation_id)
    })
}

impl Serialize for V2OperationCollection {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(S::Error::custom)?;
        let mut state = serializer.serialize_struct("V2OperationCollection", 5)?;
        state.serialize_field("schema_version", &self.schema_version)?;
        state.serialize_field("epoch", &self.epoch)?;
        state.serialize_field("revision", &self.revision)?;
        state.serialize_field("generated_at_unix_ms", &self.generated_at_unix_ms)?;
        state.serialize_field("operations", &self.operations)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for V2OperationCollection {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            #[serde(deserialize_with = "deserialize_schema_version")]
            schema_version: u32,
            epoch: StreamEpoch,
            revision: DecimalU64,
            generated_at_unix_ms: DecimalU64,
            operations: Vec<V2Operation>,
        }
        let wire = Wire::deserialize(deserializer)?;
        let value = Self {
            schema_version: wire.schema_version,
            epoch: wire.epoch,
            revision: wire.revision,
            generated_at_unix_ms: wire.generated_at_unix_ms,
            operations: wire.operations,
        };
        value.validate().map_err(de::Error::custom)?;
        Ok(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V2OperationEnvelope {
    pub schema_version: u32,
    pub epoch: StreamEpoch,
    pub revision: DecimalU64,
    pub generated_at_unix_ms: DecimalU64,
    pub operation: V2Operation,
}

impl V2OperationEnvelope {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.schema_version != V2_SCHEMA_VERSION
            || self.revision.get() == 0
            || self.operation.updated_revision > self.revision
            || self.operation.updated_at_unix_ms > self.generated_at_unix_ms
        {
            Err("operation lies beyond its published envelope")
        } else {
            Ok(())
        }
    }
}

impl Serialize for V2OperationEnvelope {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(S::Error::custom)?;
        let mut state = serializer.serialize_struct("V2OperationEnvelope", 5)?;
        state.serialize_field("schema_version", &self.schema_version)?;
        state.serialize_field("epoch", &self.epoch)?;
        state.serialize_field("revision", &self.revision)?;
        state.serialize_field("generated_at_unix_ms", &self.generated_at_unix_ms)?;
        state.serialize_field("operation", &self.operation)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for V2OperationEnvelope {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            #[serde(deserialize_with = "deserialize_schema_version")]
            schema_version: u32,
            epoch: StreamEpoch,
            revision: DecimalU64,
            generated_at_unix_ms: DecimalU64,
            operation: V2Operation,
        }
        let wire = Wire::deserialize(deserializer)?;
        let value = Self {
            schema_version: wire.schema_version,
            epoch: wire.epoch,
            revision: wire.revision,
            generated_at_unix_ms: wire.generated_at_unix_ms,
            operation: wire.operation,
        };
        value.validate().map_err(de::Error::custom)?;
        Ok(value)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct V2StreamPosition {
    pub epoch: StreamEpoch,
    pub cursor: DecimalU64,
    pub cursor_gap: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V2ControlEvent {
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
        if self.schema_version != V2_SCHEMA_VERSION {
            return Err("unsupported event schema version");
        }
        if self.sequence.get() == 0 || self.revision.get() == 0 || self.sequence > self.revision {
            return Err("invalid event sequence or revision");
        }
        if self.node.is_none() && self.slot.is_none() && self.operation.is_none() {
            return Err("event has no committed record");
        }
        let expected_slot_id = self.slot.as_ref().map(|slot| slot.slot_id).or_else(|| {
            self.operation
                .as_ref()
                .and_then(|operation| operation.slot_id)
        });
        let expected_operation_id = self
            .operation
            .as_ref()
            .map(|operation| operation.operation_id)
            .or_else(|| self.slot.as_ref().and_then(|slot| slot.operation_id));
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
            || self
                .operation
                .as_ref()
                .is_some_and(|operation| operation.updated_revision != self.revision)
            || self
                .operation
                .as_ref()
                .is_some_and(|operation| operation.updated_at_unix_ms > self.committed_at_unix_ms)
            || self.slot_id != expected_slot_id
            || self.operation_id != expected_operation_id
            || (self.operation.is_some() && self.node_instance_id.is_none())
            || self
                .node
                .as_ref()
                .is_some_and(|node| Some(node.node_instance_id) != self.node_instance_id)
            || self
                .slot
                .as_ref()
                .and_then(|slot| slot.operation_id)
                .is_some_and(|operation_id| Some(operation_id) != self.operation_id)
            || self.operation.as_ref().is_some_and(|operation| {
                operation.slot_id != self.slot_id
                    || operation
                        .slot_id
                        .zip(self.slot.as_ref().map(|slot| slot.slot_id))
                        .is_some_and(|(operation_slot, slot)| operation_slot != slot)
            })
        {
            return Err("event correlation mismatch");
        }
        let entity_matches = match self.entity {
            V2EventEntity::Node => {
                self.node_id.to_string() == self.entity_id
                    && self.node.is_some()
                    && self.slot.is_none()
                    && self.operation.is_none()
            }
            V2EventEntity::Slot => {
                self.slot_id
                    .is_some_and(|id| id.to_string() == self.entity_id)
                    && self.slot.is_some()
                    && self.operation.is_none()
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

impl Serialize for V2ControlEvent {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(S::Error::custom)?;
        let mut state = serializer.serialize_struct("V2ControlEvent", 15)?;
        state.serialize_field("schema_version", &self.schema_version)?;
        state.serialize_field("event_id", &self.event_id)?;
        state.serialize_field("epoch", &self.epoch)?;
        state.serialize_field("sequence", &self.sequence)?;
        state.serialize_field("revision", &self.revision)?;
        state.serialize_field("committed_at_unix_ms", &self.committed_at_unix_ms)?;
        state.serialize_field("entity", &self.entity)?;
        state.serialize_field("entity_id", &self.entity_id)?;
        state.serialize_field("node_id", &self.node_id)?;
        state.serialize_field("node_instance_id", &self.node_instance_id)?;
        state.serialize_field("slot_id", &self.slot_id)?;
        state.serialize_field("operation_id", &self.operation_id)?;
        state.serialize_field("node", &self.node)?;
        state.serialize_field("slot", &self.slot)?;
        state.serialize_field("operation", &self.operation)?;
        state.end()
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V2ReconnectSnapshot {
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

impl V2ReconnectSnapshot {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.schema_version != V2_SCHEMA_VERSION
            || self.stream.epoch != self.epoch
            || self.stream.cursor.get() == 0
            || self.revision.get() == 0
            || self.stream.cursor > self.revision
            || (self.stream.cursor_gap && !self.events.is_empty())
            || self.nodes.len() != 1
            || self.slots.len() != 1
            || self.operations.len() > MAX_PUBLIC_OPERATIONS
            || self.slots[0].node_id != self.nodes[0].node_id
        {
            return Err("invalid capacity-one reconnect snapshot");
        }
        let node_id = self.nodes[0].node_id;
        let slot = &self.slots[0];
        let mut operation_ids = HashSet::with_capacity(self.operations.len());
        let mut active_lifecycle_operation = None;
        for operation in &self.operations {
            if operation.node_id != node_id
                || operation.updated_revision > self.revision
                || operation.updated_at_unix_ms > self.generated_at_unix_ms
                || !operation_ids.insert(operation.operation_id)
            {
                return Err("snapshot operation correlation mismatch");
            }
            if matches!(
                operation.kind,
                V2OperationKind::Load | V2OperationKind::Unload
            ) && matches!(
                operation.status,
                V2OperationStatus::Queued
                    | V2OperationStatus::Running
                    | V2OperationStatus::Cancelling
            ) && active_lifecycle_operation.replace(operation).is_some()
            {
                return Err("multiple active lifecycle operations in capacity-one snapshot");
            }
        }
        if !operations_are_canonical(&self.operations) {
            return Err("snapshot operations are not canonically ordered");
        }
        let slot_operation_matches = match (slot.status, active_lifecycle_operation) {
            (V2SlotStatus::Loading, Some(operation)) => {
                operation.kind == V2OperationKind::Load
                    && operation.slot_id == Some(slot.slot_id)
                    && slot.operation_id == Some(operation.operation_id)
            }
            (V2SlotStatus::Unloading, Some(operation)) => {
                operation.kind == V2OperationKind::Unload
                    && operation.slot_id == Some(slot.slot_id)
                    && slot.operation_id == Some(operation.operation_id)
            }
            (V2SlotStatus::Unloaded | V2SlotStatus::Ready | V2SlotStatus::Recovery, None) => true,
            _ => false,
        };
        if !slot_operation_matches {
            return Err("slot operation correlation mismatch");
        }
        let mut event_ids = HashSet::with_capacity(self.events.len());
        let mut previous_position = None;
        for event in &self.events {
            if event.epoch != self.epoch
                || event.node_id != node_id
                || event.sequence > self.stream.cursor
                || event.revision > self.revision
                || event.committed_at_unix_ms > self.generated_at_unix_ms
                || !event_ids.insert(event.event_id)
                || previous_position.is_some_and(
                    |(previous_sequence, previous_revision): (DecimalU64, DecimalU64)| {
                        previous_sequence.checked_next() != Some(event.sequence)
                            || previous_revision.checked_next() != Some(event.revision)
                    },
                )
            {
                return Err("snapshot event correlation mismatch");
            }
            previous_position = Some((event.sequence, event.revision));
        }
        if !self.stream.cursor_gap
            && self.events.last().is_some_and(|event| {
                event.sequence != self.stream.cursor || event.revision != self.revision
            })
        {
            return Err("snapshot event tail mismatch");
        }
        Ok(())
    }
}

impl Serialize for V2ReconnectSnapshot {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.validate().map_err(S::Error::custom)?;
        let mut state = serializer.serialize_struct("V2ReconnectSnapshot", 9)?;
        state.serialize_field("schema_version", &self.schema_version)?;
        state.serialize_field("epoch", &self.epoch)?;
        state.serialize_field("revision", &self.revision)?;
        state.serialize_field("generated_at_unix_ms", &self.generated_at_unix_ms)?;
        state.serialize_field("stream", &self.stream)?;
        state.serialize_field("nodes", &self.nodes)?;
        state.serialize_field("slots", &self.slots)?;
        state.serialize_field("operations", &self.operations)?;
        state.serialize_field("events", &self.events)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for V2ReconnectSnapshot {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            #[serde(deserialize_with = "deserialize_schema_version")]
            schema_version: u32,
            epoch: StreamEpoch,
            revision: DecimalU64,
            generated_at_unix_ms: DecimalU64,
            stream: V2StreamPosition,
            nodes: Vec<V2Node>,
            slots: Vec<V2Slot>,
            operations: Vec<V2Operation>,
            events: Vec<V2ControlEvent>,
        }
        let wire = Wire::deserialize(deserializer)?;
        let value = Self {
            schema_version: wire.schema_version,
            epoch: wire.epoch,
            revision: wire.revision,
            generated_at_unix_ms: wire.generated_at_unix_ms,
            stream: wire.stream,
            nodes: wire.nodes,
            slots: wire.slots,
            operations: wire.operations,
            events: wire.events,
        };
        value.validate().map_err(de::Error::custom)?;
        Ok(value)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct V2OperationAccepted {
    pub epoch: StreamEpoch,
    pub operation_id: OperationId,
    #[serde(
        deserialize_with = "deserialize_nonzero_decimal_u64",
        serialize_with = "serialize_nonzero_decimal_u64"
    )]
    pub revision: DecimalU64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct V2ControlErrorBody {
    pub code: V2ControlErrorCode,
    #[serde(
        deserialize_with = "deserialize_public_message",
        serialize_with = "serialize_public_message"
    )]
    pub message: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct V2LoadRequest {
    #[serde(
        deserialize_with = "deserialize_model_id",
        serialize_with = "serialize_model_id"
    )]
    pub model_id: String,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct V2EmptyRequest {}
