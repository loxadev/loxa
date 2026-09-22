use serde::{Deserialize, Deserializer, Serialize};

mod drafts;
mod generation;
mod generation_status;
mod history;
mod settings;

pub use drafts::{DraftCommand, DraftReply, DraftSnapshot, MAX_DRAFT_TEXT_BYTES};
pub use generation::{
    GenerationAccepted, GenerationCommand, GenerationConnection, GenerationDraft, GenerationHello,
    GenerationHelloAck, GenerationReply, GenerationTarget, MAX_GENERATION_USER_TEXT_BYTES,
};
pub use generation_status::{GenerationExecutionPhase, GenerationSavePhase, GenerationStatus};
pub use history::{
    AttemptExecution, AttemptSave, AttemptStatistics, AttemptStopReason, AttemptSummary,
    ContentRange, ContentSource, ConversationCursor, ConversationPage, ConversationSummary,
    EngineDecodeRate, HistoryCommand, HistoryPhase, HistoryReply, HistoryStatus, TurnCursor,
    TurnPage, TurnSummary, HISTORY_SCHEMA_VERSION, MAX_CONTENT_RANGE_BYTES,
    MAX_CONVERSATION_PAGE_BYTES, MAX_CONVERSATION_PAGE_ITEMS, MAX_CONVERSATION_TITLE_BYTES,
    MAX_TURN_PAGE_BYTES, MAX_TURN_PAGE_ITEMS,
};
pub use settings::{
    ConversationProfile, GenerationSettings, GenerationSettingsPatch, OptionalU16Patch,
    OptionalU32Patch, ServiceSettings, ServiceSettingsApplication, ServiceSettingsCommand,
    ServiceSettingsDurability, ServiceSettingsPatch, ServiceSettingsReply,
};

pub const PROTOCOL_MAJOR: u16 = 1;
pub const PROTOCOL_MINOR: u16 = 4;

pub const MAX_BUILD_BYTES: usize = 96;
pub const MAX_ID_BYTES: usize = 160;
pub const MAX_PATH_BYTES: usize = 1024;
pub const MAX_ERROR_CONTEXT_BYTES: usize = 512;
pub const MAX_CAPABILITIES: usize = 8;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProtocolVersion {
    pub major: u16,
    pub minor: u16,
}

impl ProtocolVersion {
    pub const V1_0: Self = Self { major: 1, minor: 0 };
    pub const V1_1: Self = Self { major: 1, minor: 1 };
    pub const V1_2: Self = Self { major: 1, minor: 2 };
    pub const V1_3: Self = Self { major: 1, minor: 3 };
    pub const V1_4: Self = Self { major: 1, minor: 4 };

    pub const CURRENT: Self = Self {
        major: PROTOCOL_MAJOR,
        minor: PROTOCOL_MINOR,
    };
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    Status,
    Load,
    Unload,
    StopService,
    EngineUnixSocket,
    History,
    Drafts,
    Settings,
}

pub const REQUIRED_CAPABILITIES: [Capability; 5] = [
    Capability::Status,
    Capability::Load,
    Capability::Unload,
    Capability::StopService,
    Capability::EngineUnixSocket,
];

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Hello {
    pub protocol: ProtocolVersion,
    pub required_capabilities: Vec<Capability>,
    pub build: String,
    pub root_identity: String,
    #[serde(
        default,
        deserialize_with = "deserialize_present",
        skip_serializing_if = "Option::is_none"
    )]
    pub generation: Option<GenerationHello>,
}

impl Hello {
    pub fn current(build: impl Into<String>, root_identity: impl Into<String>) -> Self {
        Self {
            protocol: ProtocolVersion::V1_0,
            required_capabilities: REQUIRED_CAPABILITIES.to_vec(),
            build: build.into(),
            root_identity: root_identity.into(),
            generation: None,
        }
    }

    pub fn history_status(build: impl Into<String>, root_identity: impl Into<String>) -> Self {
        Self::history(build, root_identity)
    }

    pub fn history(build: impl Into<String>, root_identity: impl Into<String>) -> Self {
        Self {
            protocol: ProtocolVersion::CURRENT,
            // Protocol 1.0 peers have a closed capability enum. Keep this
            // initial vocabulary decodable so they can return the typed
            // protocol mismatch before a 1.1 client asks for history.
            required_capabilities: REQUIRED_CAPABILITIES.to_vec(),
            build: build.into(),
            root_identity: root_identity.into(),
            generation: None,
        }
    }

    pub fn generation(
        build: impl Into<String>,
        root_identity: impl Into<String>,
        connection: GenerationConnection,
    ) -> Self {
        Self {
            protocol: ProtocolVersion::V1_2,
            // Generation is minor-version gated so the closed 1.0 capability
            // vocabulary stays decodable and remains at eight entries.
            required_capabilities: REQUIRED_CAPABILITIES.to_vec(),
            build: build.into(),
            root_identity: root_identity.into(),
            generation: Some(GenerationHello { connection }),
        }
    }

    pub fn generation_status(build: impl Into<String>, root_identity: impl Into<String>) -> Self {
        Self {
            protocol: ProtocolVersion::V1_3,
            ..Self::current(build, root_identity)
        }
    }

    pub fn validate_shape(&self) -> Result<(), &'static str> {
        if self.build.is_empty() || self.build.len() > MAX_BUILD_BYTES {
            return Err("invalid client build identity");
        }
        if self.root_identity.is_empty() || self.root_identity.len() > MAX_ID_BYTES {
            return Err("invalid development root identity");
        }
        if self.required_capabilities.len() > MAX_CAPABILITIES {
            return Err("too many required capabilities");
        }
        if self.generation.is_some() && self.protocol != ProtocolVersion::V1_2 {
            return Err("generation handshake requires service protocol 1.2");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HelloAck {
    pub protocol: ProtocolVersion,
    pub capabilities: Vec<Capability>,
    pub build: String,
    pub storage_schema: u32,
    pub boot_epoch: String,
    pub root_identity: String,
    pub service_pid: u32,
    pub origin_sha256: String,
    #[serde(
        default,
        deserialize_with = "deserialize_present",
        skip_serializing_if = "Option::is_none"
    )]
    pub generation: Option<GenerationHelloAck>,
}

impl HelloAck {
    pub fn validate_shape(&self) -> Result<(), &'static str> {
        if self.capabilities.len() > MAX_CAPABILITIES {
            return Err("too many negotiated capabilities");
        }
        if self.build.is_empty() || self.build.len() > MAX_BUILD_BYTES {
            return Err("invalid service build identity");
        }
        if self.boot_epoch.is_empty() || self.boot_epoch.len() > MAX_ID_BYTES {
            return Err("invalid service boot epoch");
        }
        if self.root_identity.is_empty() || self.root_identity.len() > MAX_ID_BYTES {
            return Err("invalid development root identity");
        }
        if self.service_pid == 0 || !is_lower_hex_64(&self.origin_sha256) {
            return Err("invalid service process identity");
        }
        match (&self.generation, self.protocol) {
            (Some(generation), ProtocolVersion::V1_2) => generation.validate_shape()?,
            (Some(_), _) => return Err("generation acknowledgement requires protocol 1.2"),
            (None, _) => {}
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientEnvelope {
    Hello(Hello),
    Request(Request),
    Subscribe { request_id: String },
}

impl ClientEnvelope {
    pub fn validate_shape(&self) -> Result<(), &'static str> {
        match self {
            Self::Hello(hello) => hello.validate_shape(),
            Self::Request(request) => request.validate_shape(),
            Self::Subscribe { request_id } => {
                validate_identifier(request_id, "invalid subscription request identity")
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerEnvelope {
    HelloAck(HelloAck),
    HelloRejected(ServiceError),
    Reply(Reply),
    Snapshot(RuntimeStatus),
}

impl ServerEnvelope {
    pub fn validate_shape(&self) -> Result<(), &'static str> {
        match self {
            Self::HelloAck(ack) => ack.validate_shape(),
            Self::HelloRejected(error) => error.validate_shape(),
            Self::Reply(reply) => reply.validate_shape(),
            Self::Snapshot(status) => status.validate_shape(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub request_id: String,
    pub command: ServiceCommand,
}

impl Request {
    pub fn new(request_id: impl Into<String>, command: ServiceCommand) -> Self {
        Self {
            request_id: request_id.into(),
            command,
        }
    }

    pub fn validate_shape(&self) -> Result<(), &'static str> {
        if self.request_id.is_empty() || self.request_id.len() > MAX_ID_BYTES {
            return Err("invalid request identity");
        }
        self.command.validate_shape()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServiceCommand {
    Status,
    Load { model_id: String },
    Unload { target: OperationTarget },
    StopService,
    History { command: HistoryCommand },
    Draft { command: DraftCommand },
    Settings { command: ServiceSettingsCommand },
    Generation { command: GenerationCommand },
    GetGenerationStatus { target: GenerationTarget },
}

impl ServiceCommand {
    pub fn validate_shape(&self) -> Result<(), &'static str> {
        match self {
            Self::Load { model_id } => validate_model_id(model_id),
            Self::Unload { target } => target.validate_shape(),
            Self::History { command } => command.validate_shape(),
            Self::Draft { command } => command.validate_shape(),
            Self::Settings { command } => command.validate_shape(),
            Self::Generation { command } => command.validate_shape(),
            Self::GetGenerationStatus { target } => target.validate_shape(),
            _ => Ok(()),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OperationTarget {
    pub boot_epoch: String,
    pub task_id: String,
    pub generation: String,
}

impl OperationTarget {
    pub fn validate_shape(&self) -> Result<(), &'static str> {
        validate_identifier(&self.boot_epoch, "invalid service boot epoch")?;
        validate_decimal(&self.task_id, "invalid task identity")?;
        validate_decimal(&self.generation, "invalid operation generation")
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Reply {
    pub request_id: String,
    pub outcome: ReplyOutcome,
}

impl Reply {
    pub fn validate_shape(&self) -> Result<(), &'static str> {
        if self.request_id.is_empty() || self.request_id.len() > MAX_ID_BYTES {
            return Err("invalid reply request identity");
        }
        match &self.outcome {
            ReplyOutcome::Status(status) => status.validate_shape(),
            ReplyOutcome::Accepted(accepted) => accepted.validate_shape(),
            ReplyOutcome::Rejected(error) => error.validate_shape(),
            ReplyOutcome::History { reply } => reply.validate_shape(),
            ReplyOutcome::Draft { reply } => reply.validate_shape(),
            ReplyOutcome::Settings { reply } => reply.validate_shape(),
            ReplyOutcome::Generation { reply } => reply.validate_shape(),
            ReplyOutcome::GenerationStatus { snapshot } => snapshot
                .as_ref()
                .map_or(Ok(()), GenerationStatus::validate_shape),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ReplyOutcome {
    Status(ServiceStatus),
    Accepted(Accepted),
    Rejected(ServiceError),
    History { reply: HistoryReply },
    Draft { reply: DraftReply },
    Settings { reply: ServiceSettingsReply },
    Generation { reply: GenerationReply },
    GenerationStatus { snapshot: Option<GenerationStatus> },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Accepted {
    pub boot_epoch: String,
    pub task_id: String,
    pub generation: String,
    pub state_revision: String,
}

impl Accepted {
    fn validate_shape(&self) -> Result<(), &'static str> {
        validate_identifier(&self.boot_epoch, "invalid service boot epoch")?;
        validate_decimal(&self.task_id, "invalid task identity")?;
        validate_decimal(&self.generation, "invalid operation generation")?;
        validate_decimal(&self.state_revision, "invalid state revision")
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeStatus {
    pub boot_epoch: String,
    pub state_revision: String,
    pub phase: RuntimePhase,
}

impl RuntimeStatus {
    fn validate_shape(&self) -> Result<(), &'static str> {
        if self.boot_epoch.is_empty() || self.boot_epoch.len() > MAX_ID_BYTES {
            return Err("invalid service boot epoch");
        }
        validate_decimal(&self.state_revision, "invalid state revision")?;
        self.phase.validate_shape()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceStatus {
    pub runtime: RuntimeStatus,
    /// A point-in-time sample for this explicit Status response. Diagnostic
    /// health does not participate in the runtime state revision or stream.
    pub diagnostics: DiagnosticsStatus,
}

impl ServiceStatus {
    fn validate_shape(&self) -> Result<(), &'static str> {
        self.runtime.validate_shape()?;
        self.diagnostics.validate_shape()
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticsStatus {
    /// Whether the sink can currently accept records. Cumulative counters can
    /// remain nonzero after availability recovers on a later daily file.
    pub available: bool,
    pub enqueue_drops: u64,
    pub sink_failures: u64,
    pub sink_discards: u64,
    pub at_capacity: bool,
    pub sink_failed: bool,
}

impl DiagnosticsStatus {
    fn validate_shape(&self) -> Result<(), &'static str> {
        if self.available && (self.at_capacity || self.sink_failed) {
            Err("invalid diagnostics availability")
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RuntimePhase {
    Unloaded,
    Starting {
        task_id: String,
        generation: String,
        model_id: String,
    },
    Ready {
        task_id: String,
        generation: String,
        model_id: String,
        engine_pid: u32,
    },
    Stopping {
        task_id: String,
        generation: String,
        model_id: String,
    },
    CleanupFailed {
        task_id: String,
        generation: String,
        model_id: String,
    },
    LoadFailed {
        task_id: String,
        generation: String,
        model_id: String,
        category: ErrorCategory,
    },
    RecoveryRequired {
        reason: String,
    },
    Draining,
}

impl RuntimePhase {
    fn validate_shape(&self) -> Result<(), &'static str> {
        match self {
            Self::Starting {
                task_id,
                generation,
                model_id,
            }
            | Self::Stopping {
                task_id,
                generation,
                model_id,
            }
            | Self::CleanupFailed {
                task_id,
                generation,
                model_id,
            }
            | Self::LoadFailed {
                task_id,
                generation,
                model_id,
                ..
            } => {
                validate_decimal(task_id, "invalid task identity")?;
                validate_decimal(generation, "invalid operation generation")?;
                validate_model_id(model_id)
            }
            Self::Ready {
                task_id,
                generation,
                model_id,
                engine_pid,
            } => {
                validate_decimal(task_id, "invalid task identity")?;
                validate_decimal(generation, "invalid operation generation")?;
                validate_model_id(model_id)?;
                if *engine_pid == 0 {
                    return Err("invalid engine process identity");
                }
                Ok(())
            }
            Self::RecoveryRequired { reason } => {
                if reason.is_empty() || reason.len() > MAX_ERROR_CONTEXT_BYTES {
                    Err("invalid recovery reason")
                } else {
                    Ok(())
                }
            }
            Self::Unloaded | Self::Draining => Ok(()),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCategory {
    Busy,
    Conflict,
    NotFound,
    IncompatibleProtocol,
    UnsupportedCapability,
    HomeMismatch,
    InvalidRequest,
    ServiceUnavailable,
    RecoveryRequired,
    ModelUnavailable,
    StartupFailed,
    Internal,
    OutcomeUnknown,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceError {
    pub category: ErrorCategory,
    pub context: String,
}

impl ServiceError {
    pub fn new(category: ErrorCategory, context: impl Into<String>) -> Self {
        let mut context = context.into();
        if context.len() > MAX_ERROR_CONTEXT_BYTES {
            let mut boundary = MAX_ERROR_CONTEXT_BYTES;
            while !context.is_char_boundary(boundary) {
                boundary -= 1;
            }
            context.truncate(boundary);
        }
        Self { category, context }
    }

    pub fn validate_shape(&self) -> Result<(), &'static str> {
        if self.context.is_empty() || self.context.len() > MAX_ERROR_CONTEXT_BYTES {
            Err("invalid error context")
        } else {
            Ok(())
        }
    }
}

fn validate_decimal(value: &str, error: &'static str) -> Result<(), &'static str> {
    if value.is_empty()
        || value.len() > 20
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
        || value.parse::<u64>().is_err()
    {
        Err(error)
    } else {
        Ok(())
    }
}

fn validate_identifier(value: &str, error: &'static str) -> Result<(), &'static str> {
    if value.is_empty() || value.len() > MAX_ID_BYTES {
        Err(error)
    } else {
        Ok(())
    }
}

fn validate_model_id(model_id: &str) -> Result<(), &'static str> {
    if model_id.is_empty()
        || model_id.len() > 120
        || !model_id
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        || model_id.starts_with('-')
        || model_id.ends_with('-')
    {
        Err("invalid model identity")
    } else {
        Ok(())
    }
}

fn is_lower_hex_64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn deserialize_present<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Deserialize, Eq, PartialEq)]
    #[serde(deny_unknown_fields)]
    struct OldProtocolVersion {
        major: u16,
        minor: u16,
    }

    #[derive(Debug, Deserialize, Eq, PartialEq)]
    #[serde(rename_all = "snake_case")]
    enum OldCapability {
        Status,
        Load,
        Unload,
        StopService,
        EngineUnixSocket,
    }

    #[derive(Debug, Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case")]
    enum OldClientEnvelope {
        Hello {
            protocol: OldProtocolVersion,
            required_capabilities: Vec<OldCapability>,
            build: String,
            root_identity: String,
        },
    }

    #[derive(Debug, Deserialize)]
    #[serde(tag = "type", rename_all = "snake_case")]
    enum OldServerEnvelope {
        HelloAck {
            protocol: OldProtocolVersion,
            capabilities: Vec<OldCapability>,
            build: String,
            storage_schema: u32,
            boot_epoch: String,
            root_identity: String,
            service_pid: u32,
            origin_sha256: String,
        },
    }

    #[test]
    fn generation_control_stays_on_1_2_while_status_uses_an_ordinary_1_3_hello() {
        for connection in [GenerationConnection::Request, GenerationConnection::Control] {
            let mut hello = Hello::generation("build", "root", connection);
            assert_eq!(hello.protocol, ProtocolVersion::V1_2);
            hello.validate_shape().unwrap();
            hello.protocol = ProtocolVersion::CURRENT;
            assert!(hello.validate_shape().is_err());
        }
        let hello = Hello::generation_status("build", "root");
        assert_eq!(hello.protocol, ProtocolVersion { major: 1, minor: 3 });
        assert!(hello.generation.is_none());
        assert_eq!(hello.required_capabilities, REQUIRED_CAPABILITIES);
        hello.validate_shape().unwrap();
        let mut ack = HelloAck {
            protocol: ProtocolVersion::V1_2,
            capabilities: REQUIRED_CAPABILITIES.to_vec(),
            build: "build".into(),
            storage_schema: 0,
            boot_epoch: "boot".into(),
            root_identity: "root".into(),
            service_pid: 1,
            origin_sha256: "aa".repeat(32),
            generation: Some(GenerationHelloAck {
                pending_nonce: "11".repeat(16),
            }),
        };
        ack.validate_shape().unwrap();
        ack.protocol = hello.protocol;
        assert!(ack.validate_shape().is_err());
        ack.generation = None;
        ack.validate_shape().unwrap();
    }

    #[test]
    fn genuine_protocol_1_0_hello_fixture_still_decodes() {
        let fixture = br#"{
            "type":"hello",
            "protocol":{"major":1,"minor":0},
            "required_capabilities":["status","load","unload","stop_service","engine_unix_socket"],
            "build":"0.1.0-dev",
            "root_identity":"root"
        }"#;
        let envelope: ClientEnvelope = serde_json::from_slice(fixture).unwrap();
        assert_eq!(
            envelope,
            ClientEnvelope::Hello(Hello::current("0.1.0-dev", "root"))
        );
    }

    #[test]
    fn legacy_hello_and_ack_reject_an_explicit_generation_null() {
        let hello = br#"{
            "type":"hello",
            "protocol":{"major":1,"minor":0},
            "required_capabilities":["status","load","unload","stop_service","engine_unix_socket"],
            "build":"0.1.0-dev",
            "root_identity":"root",
            "generation":null
        }"#;
        assert!(serde_json::from_slice::<ClientEnvelope>(hello).is_err());

        let ack = br#"{
            "type":"hello_ack",
            "protocol":{"major":1,"minor":1},
            "capabilities":["status","load","unload","stop_service","engine_unix_socket"],
            "build":"0.1.0-dev",
            "storage_schema":1,
            "boot_epoch":"boot",
            "root_identity":"root",
            "service_pid":1,
            "origin_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "generation":null
        }"#;
        assert!(serde_json::from_slice::<ServerEnvelope>(ack).is_err());
    }

    #[test]
    fn legacy_hello_and_ack_projection_remain_decodable_by_closed_old_shapes() {
        let encoded =
            serde_json::to_vec(&ClientEnvelope::Hello(Hello::current("0.1.0-dev", "root")))
                .unwrap();
        let OldClientEnvelope::Hello {
            protocol,
            required_capabilities,
            build,
            root_identity,
        } = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(protocol, OldProtocolVersion { major: 1, minor: 0 });
        assert_eq!(required_capabilities.len(), 5);
        assert_eq!(build, "0.1.0-dev");
        assert_eq!(root_identity, "root");

        let ack = ServerEnvelope::HelloAck(HelloAck {
            protocol: ProtocolVersion::V1_0,
            capabilities: REQUIRED_CAPABILITIES.to_vec(),
            build: "0.1.0-dev".into(),
            storage_schema: 1,
            boot_epoch: "boot".into(),
            root_identity: "root".into(),
            service_pid: 1,
            origin_sha256: "a".repeat(64),
            generation: None,
        });
        let encoded = serde_json::to_vec(&ack).unwrap();
        let OldServerEnvelope::HelloAck {
            protocol,
            capabilities,
            build,
            storage_schema,
            boot_epoch,
            root_identity,
            service_pid,
            origin_sha256,
        } = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(protocol, OldProtocolVersion { major: 1, minor: 0 });
        assert_eq!(capabilities.len(), 5);
        assert_eq!(build, "0.1.0-dev");
        assert_eq!(storage_schema, 1);
        assert_eq!(boot_epoch, "boot");
        assert_eq!(root_identity, "root");
        assert_eq!(service_pid, 1);
        assert_eq!(origin_sha256, "a".repeat(64));
    }

    #[test]
    fn history_hellos_request_the_current_protocol_through_the_closed_old_shape() {
        for hello in [
            Hello::history_status("0.1.0-dev", "root"),
            Hello::history("0.1.0-dev", "root"),
        ] {
            let encoded = serde_json::to_vec(&ClientEnvelope::Hello(hello)).unwrap();
            let OldClientEnvelope::Hello {
                protocol,
                required_capabilities,
                ..
            } = serde_json::from_slice(&encoded).unwrap();
            assert_eq!(protocol, OldProtocolVersion { major: 1, minor: 4 });
            assert_eq!(required_capabilities.len(), REQUIRED_CAPABILITIES.len());
        }
    }

    #[test]
    fn conversation_pages_reject_a_fifty_first_item_during_decode() {
        let item = ConversationSummary {
            id: "a".repeat(32),
            model_id: "demo".into(),
            title: "chat".into(),
            created_ms: "1".into(),
            updated_ms: "1".into(),
            revision: "1".into(),
            profile_revision: "1".into(),
        };
        let encoded = serde_json::json!({
            "conversations": vec![item; MAX_CONVERSATION_PAGE_ITEMS + 1],
            "next": null
        });
        assert!(serde_json::from_value::<ConversationPage>(encoded).is_err());
    }

    #[test]
    fn history_commands_and_replies_have_one_tag_at_each_wire_level() {
        let request = ClientEnvelope::Request(Request::new(
            "history-1",
            ServiceCommand::History {
                command: HistoryCommand::ListConversations {
                    cursor: None,
                    limit: 1,
                },
            },
        ));
        let encoded = serde_json::to_value(&request).unwrap();
        assert_eq!(encoded["type"], "request");
        assert_eq!(encoded["command"]["type"], "history");
        assert_eq!(encoded["command"]["command"]["type"], "list_conversations");
        assert_eq!(
            serde_json::from_value::<ClientEnvelope>(encoded).unwrap(),
            request
        );

        let response = ServerEnvelope::Reply(Reply {
            request_id: "history-1".into(),
            outcome: ReplyOutcome::History {
                reply: HistoryReply::ConversationPage(ConversationPage {
                    conversations: Vec::new(),
                    next: None,
                }),
            },
        });
        let encoded = serde_json::to_value(&response).unwrap();
        assert_eq!(encoded["type"], "reply");
        assert_eq!(encoded["outcome"]["type"], "history");
        assert_eq!(encoded["outcome"]["reply"]["type"], "conversation_page");
        assert_eq!(
            serde_json::from_value::<ServerEnvelope>(encoded).unwrap(),
            response
        );
    }

    #[test]
    fn turn_and_content_dtos_reject_impossible_public_shapes() {
        let mut valid_attempt = AttemptSummary {
            id: "b".repeat(32),
            attempt_number: "1".into(),
            execution: AttemptExecution::Completed,
            save: AttemptSave::Saved,
            saved_end: "2".into(),
            generated_end: Some("2".into()),
            terminal_saved_end: Some("2".into()),
            failure_code: None,
            statistics: None,
            created_ms: "1".into(),
            updated_ms: "2".into(),
        };
        let page = |attempt: AttemptSummary, user_text_end: &str| {
            HistoryReply::TurnPage(TurnPage {
                turns: vec![TurnSummary {
                    id: "a".repeat(32),
                    ordinal: "1".into(),
                    user_text_end: user_text_end.into(),
                    selected_attempt: Some(attempt),
                }],
                next: None,
            })
        };

        valid_attempt.statistics = Some(AttemptStatistics {
            qualified_input_tokens: Some(7),
            qualified_output_tokens: Some(2),
            service_first_output_latency_ms: Some("3".into()),
            qualified_engine_decode_tokens_per_second: EngineDecodeRate::new(25.0),
            service_total_duration_ms: "9".into(),
            stop_reason: AttemptStopReason::Completed,
        });
        let encoded = serde_json::to_value(&valid_attempt).unwrap();
        assert_eq!(encoded["statistics"]["qualified_input_tokens"], 7);
        assert_eq!(
            encoded["statistics"]["qualified_engine_decode_tokens_per_second"],
            25.0
        );
        assert_eq!(encoded["statistics"]["service_total_duration_ms"], "9");

        let mut invalid = valid_attempt.clone();
        invalid.terminal_saved_end = None;
        assert!(page(invalid, "1").validate_shape().is_err());

        let mut invalid = valid_attempt.clone();
        invalid.updated_ms = "0".into();
        assert!(page(invalid, "1").validate_shape().is_err());
        let mut invalid = valid_attempt.clone();
        invalid
            .statistics
            .as_mut()
            .unwrap()
            .service_first_output_latency_ms = Some("10".into());
        assert!(page(invalid, "1").validate_shape().is_err());
        let mut invalid = valid_attempt.clone();
        invalid.statistics.as_mut().unwrap().qualified_output_tokens = None;
        assert!(page(invalid, "1").validate_shape().is_err());
        assert!(page(valid_attempt, "32769").validate_shape().is_err());

        assert!(HistoryReply::ContentRange(ContentRange {
            start: "0".into(),
            end: "2".into(),
            prefix_end: "2".into(),
            content: "x".into(),
        })
        .validate_shape()
        .is_err());
        assert!(HistoryCommand::ReadContentRange {
            source: ContentSource::Assistant {
                attempt_id: "c".repeat(32),
            },
            start: "0".into(),
            prefix_end: "16777217".into(),
        }
        .validate_shape()
        .is_err());
    }
}
