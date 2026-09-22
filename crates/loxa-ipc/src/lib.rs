#![cfg_attr(not(unix), allow(dead_code))]

#[cfg(not(unix))]
compile_error!("loxa-ipc currently supports macOS and Linux Unix sockets only");

mod bootstrap;
mod client;
mod codec;
mod platform;
mod protocol;

pub use bootstrap::{
    initialize_development_root, ClientBootstrap, DevelopmentInstanceLock, DevelopmentRoot,
    OriginRecord, DEVELOPMENT_MARKER_FILENAME, ENGINE_SOCKET_FILENAME_BYTES,
    ENGINE_SOCKET_NONCE_BYTES,
};
pub use client::{
    ClientError, ConnectMode, PendingGenerationRequest, ServiceClient, ServiceSubscription,
};
pub use codec::{
    decode, decode_with_limit, encode, encode_with_limit, framed, set_frame_limit, IpcFramed,
    MAX_FRAME_BYTES, MAX_HISTORY_FRAME_BYTES,
};
pub use platform::{peer_credentials, PeerCredentials};
pub use protocol::{
    Accepted, AttemptExecution, AttemptSave, AttemptStatistics, AttemptStopReason, AttemptSummary,
    Capability, ClientEnvelope, ContentRange, ContentSource, ConversationCursor, ConversationPage,
    ConversationProfile, ConversationSummary, DiagnosticsStatus, DraftCommand, DraftReply,
    DraftSnapshot, EngineDecodeRate, ErrorCategory, GenerationAccepted, GenerationCommand,
    GenerationConnection, GenerationDraft, GenerationExecutionPhase, GenerationHello,
    GenerationHelloAck, GenerationReply, GenerationSavePhase, GenerationSettings,
    GenerationSettingsPatch, GenerationStatus, GenerationTarget, Hello, HelloAck, HistoryCommand,
    HistoryPhase, HistoryReply, HistoryStatus, OperationTarget, OptionalU16Patch, OptionalU32Patch,
    ProtocolVersion, Reply, ReplyOutcome, Request, RuntimePhase, RuntimeStatus, ServerEnvelope,
    ServiceCommand, ServiceError, ServiceSettings, ServiceSettingsApplication,
    ServiceSettingsCommand, ServiceSettingsDurability, ServiceSettingsPatch, ServiceSettingsReply,
    ServiceStatus, TurnCursor, TurnPage, TurnSummary, HISTORY_SCHEMA_VERSION,
    MAX_CONTENT_RANGE_BYTES, MAX_CONVERSATION_PAGE_BYTES, MAX_CONVERSATION_PAGE_ITEMS,
    MAX_CONVERSATION_TITLE_BYTES, MAX_DRAFT_TEXT_BYTES, MAX_GENERATION_USER_TEXT_BYTES,
    MAX_TURN_PAGE_BYTES, MAX_TURN_PAGE_ITEMS, PROTOCOL_MAJOR, PROTOCOL_MINOR,
};
