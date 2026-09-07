use serde::{Deserialize, Serialize};

pub const PROTOCOL_MAJOR: u16 = 1;
pub const PROTOCOL_MINOR: u16 = 0;

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
}

impl Hello {
    pub fn current(build: impl Into<String>, root_identity: impl Into<String>) -> Self {
        Self {
            protocol: ProtocolVersion::CURRENT,
            required_capabilities: REQUIRED_CAPABILITIES.to_vec(),
            build: build.into(),
            root_identity: root_identity.into(),
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
}

impl ServiceCommand {
    pub fn validate_shape(&self) -> Result<(), &'static str> {
        match self {
            Self::Load { model_id } => validate_model_id(model_id),
            Self::Unload { target } => target.validate_shape(),
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
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ReplyOutcome {
    Status(ServiceStatus),
    Accepted(Accepted),
    Rejected(ServiceError),
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
