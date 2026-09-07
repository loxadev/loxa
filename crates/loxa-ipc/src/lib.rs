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
    OriginRecord, DEVELOPMENT_MARKER_FILENAME,
};
pub use client::{ClientError, ConnectMode, ServiceClient, ServiceSubscription};
pub use codec::{decode, encode, framed, IpcFramed, MAX_FRAME_BYTES};
pub use platform::{peer_credentials, PeerCredentials};
pub use protocol::{
    Accepted, Capability, ClientEnvelope, DiagnosticsStatus, ErrorCategory, Hello, HelloAck,
    OperationTarget, ProtocolVersion, Reply, ReplyOutcome, Request, RuntimePhase, RuntimeStatus,
    ServerEnvelope, ServiceCommand, ServiceError, ServiceStatus, PROTOCOL_MAJOR, PROTOCOL_MINOR,
};
