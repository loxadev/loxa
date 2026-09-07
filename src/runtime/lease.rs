use crate::runtime_fingerprint::RuntimeFingerprint;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

pub(super) const LEGACY_LEASE_VERSION: u32 = 1;
pub(super) const PERSISTENT_LEASE_VERSION: u32 = 2;
pub(super) const LEASE_VERSION: u32 = 3;
pub(super) const SERVICE_LEASE_VERSION: u32 = 4;
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum LeaseOwnerMode {
    Foreground,
    PersistentApp,
    Service,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ServiceLeaseFields {
    pub(super) endpoint: PathBuf,
    pub(super) parallel: u16,
    pub(super) offline: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RuntimeLease {
    pub(super) version: u32,
    pub(super) owner_mode: Option<LeaseOwnerMode>,
    pub(super) fingerprint: Option<RuntimeFingerprint>,
    pub(super) managed_source: Option<PathBuf>,
    pub(super) owner_pid: u32,
    pub(super) owner_start_time: u64,
    pub(super) child_pid: u32,
    pub(super) child_start_time: u64,
    pub(super) child_pgid: i32,
    pub(super) server: PathBuf,
    pub(super) model_id: String,
    pub(super) port: u16,
    pub(super) service: Option<ServiceLeaseFields>,
}

#[derive(Deserialize)]
struct LeaseVersion {
    version: u32,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RuntimeLeaseV1 {
    pub(super) version: u32,
    pub(super) owner_pid: u32,
    pub(super) owner_start_time: u64,
    pub(super) child_pid: u32,
    pub(super) child_start_time: u64,
    pub(super) child_pgid: i32,
    pub(super) server: PathBuf,
    pub(super) model_id: String,
    pub(super) port: u16,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RuntimeLeaseV2 {
    pub(super) version: u32,
    pub(super) owner_mode: LeaseOwnerMode,
    #[serde(deserialize_with = "deserialize_explicit_fingerprint")]
    pub(super) fingerprint: Option<RuntimeFingerprint>,
    pub(super) owner_pid: u32,
    pub(super) owner_start_time: u64,
    pub(super) child_pid: u32,
    pub(super) child_start_time: u64,
    pub(super) child_pgid: i32,
    pub(super) server: PathBuf,
    pub(super) model_id: String,
    pub(super) port: u16,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RuntimeLeaseV3 {
    version: u32,
    owner_mode: LeaseOwnerMode,
    #[serde(deserialize_with = "deserialize_explicit_fingerprint")]
    fingerprint: Option<RuntimeFingerprint>,
    #[serde(deserialize_with = "deserialize_explicit_managed_source")]
    managed_source: Option<PathBuf>,
    owner_pid: u32,
    owner_start_time: u64,
    child_pid: u32,
    child_start_time: u64,
    child_pgid: i32,
    server: PathBuf,
    model_id: String,
    port: u16,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RuntimeLeaseV4 {
    version: u32,
    owner_mode: LeaseOwnerMode,
    #[serde(deserialize_with = "deserialize_explicit_fingerprint")]
    fingerprint: Option<RuntimeFingerprint>,
    #[serde(deserialize_with = "deserialize_explicit_managed_source")]
    managed_source: Option<PathBuf>,
    owner_pid: u32,
    owner_start_time: u64,
    child_pid: u32,
    child_start_time: u64,
    child_pgid: i32,
    server: PathBuf,
    model_id: String,
    port: u16,
    endpoint: PathBuf,
    parallel: u16,
    offline: bool,
}

fn deserialize_explicit_fingerprint<'de, D>(
    deserializer: D,
) -> Result<Option<RuntimeFingerprint>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::deserialize(deserializer)
}

fn deserialize_explicit_managed_source<'de, D>(deserializer: D) -> Result<Option<PathBuf>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::deserialize(deserializer)
}

impl RuntimeLease {
    #[cfg(test)]
    pub(super) fn owner_mode(&self) -> Option<LeaseOwnerMode> {
        self.owner_mode
    }

    pub(super) fn persistent_fingerprint(&self) -> Option<&RuntimeFingerprint> {
        match (self.version, self.owner_mode, self.fingerprint.as_ref()) {
            (
                PERSISTENT_LEASE_VERSION | LEASE_VERSION,
                Some(LeaseOwnerMode::PersistentApp),
                Some(fingerprint),
            ) => Some(fingerprint),
            _ => None,
        }
    }

    pub(super) fn attributed_managed_source(&self) -> &Path {
        self.managed_source.as_deref().unwrap_or(&self.server)
    }
}

pub(super) fn validate_lease(lease: &RuntimeLease) -> Result<(), String> {
    let version_and_owner_are_valid = matches!(
        (
            lease.version,
            lease.owner_mode,
            lease.fingerprint.as_ref(),
            lease.managed_source.as_ref(),
            lease.service.as_ref(),
        ),
        (LEGACY_LEASE_VERSION, None, None, None, None)
            | (
                PERSISTENT_LEASE_VERSION,
                Some(LeaseOwnerMode::Foreground),
                None,
                None,
                None,
            )
            | (
                PERSISTENT_LEASE_VERSION,
                Some(LeaseOwnerMode::PersistentApp),
                Some(_),
                None,
                None,
            )
            | (
                LEASE_VERSION,
                Some(LeaseOwnerMode::Foreground),
                None,
                _,
                None
            )
            | (
                LEASE_VERSION,
                Some(LeaseOwnerMode::PersistentApp),
                Some(_),
                _,
                None,
            )
            | (
                SERVICE_LEASE_VERSION,
                Some(LeaseOwnerMode::Service),
                Some(_),
                _,
                Some(ServiceLeaseFields {
                    parallel: 1,
                    offline: true,
                    ..
                }),
            )
    );
    if !version_and_owner_are_valid
        || lease.owner_pid == 0
        || lease.child_pid == 0
        || lease.child_pgid <= 1
        || lease.child_pgid != i32::try_from(lease.child_pid).unwrap_or(-1)
        || lease.owner_start_time == 0
        || lease.child_start_time == 0
        || lease.server.as_os_str().is_empty()
        || lease
            .managed_source
            .as_ref()
            .is_some_and(|source| !source.is_absolute() || source.as_os_str().is_empty())
        || lease.model_id.is_empty()
        || (lease.version == SERVICE_LEASE_VERSION) == (lease.port != 0)
        || lease.service.as_ref().is_some_and(|service| {
            !service.endpoint.is_absolute() || service.endpoint.as_os_str().is_empty()
        })
    {
        Err("invalid runtime lease".into())
    } else if lease.version == SERVICE_LEASE_VERSION {
        lease
            .fingerprint
            .as_ref()
            .expect("validated service lease has a fingerprint")
            .validate_service_lease(&lease.model_id)
    } else if let Some(fingerprint) = lease.persistent_fingerprint() {
        fingerprint.validate_recorded_persistent_lease(&lease.model_id)
    } else {
        Ok(())
    }
}

pub(super) fn decode_lease(bytes: &[u8]) -> Result<RuntimeLease, String> {
    let version: LeaseVersion = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
    let lease = match version.version {
        LEGACY_LEASE_VERSION => {
            let lease: RuntimeLeaseV1 =
                serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
            RuntimeLease {
                version: lease.version,
                owner_mode: None,
                fingerprint: None,
                managed_source: None,
                owner_pid: lease.owner_pid,
                owner_start_time: lease.owner_start_time,
                child_pid: lease.child_pid,
                child_start_time: lease.child_start_time,
                child_pgid: lease.child_pgid,
                server: lease.server,
                model_id: lease.model_id,
                port: lease.port,
                service: None,
            }
        }
        PERSISTENT_LEASE_VERSION => {
            let lease: RuntimeLeaseV2 =
                serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
            RuntimeLease {
                version: lease.version,
                owner_mode: Some(lease.owner_mode),
                fingerprint: lease.fingerprint,
                managed_source: None,
                owner_pid: lease.owner_pid,
                owner_start_time: lease.owner_start_time,
                child_pid: lease.child_pid,
                child_start_time: lease.child_start_time,
                child_pgid: lease.child_pgid,
                server: lease.server,
                model_id: lease.model_id,
                port: lease.port,
                service: None,
            }
        }
        LEASE_VERSION => {
            let lease: RuntimeLeaseV3 =
                serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
            RuntimeLease {
                version: lease.version,
                owner_mode: Some(lease.owner_mode),
                fingerprint: lease.fingerprint,
                managed_source: lease.managed_source,
                owner_pid: lease.owner_pid,
                owner_start_time: lease.owner_start_time,
                child_pid: lease.child_pid,
                child_start_time: lease.child_start_time,
                child_pgid: lease.child_pgid,
                server: lease.server,
                model_id: lease.model_id,
                port: lease.port,
                service: None,
            }
        }
        SERVICE_LEASE_VERSION => {
            let lease: RuntimeLeaseV4 =
                serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
            RuntimeLease {
                version: lease.version,
                owner_mode: Some(lease.owner_mode),
                fingerprint: lease.fingerprint,
                managed_source: lease.managed_source,
                owner_pid: lease.owner_pid,
                owner_start_time: lease.owner_start_time,
                child_pid: lease.child_pid,
                child_start_time: lease.child_start_time,
                child_pgid: lease.child_pgid,
                server: lease.server,
                model_id: lease.model_id,
                port: lease.port,
                service: Some(ServiceLeaseFields {
                    endpoint: lease.endpoint,
                    parallel: lease.parallel,
                    offline: lease.offline,
                }),
            }
        }
        _ => return Err("unsupported runtime lease version".into()),
    };
    validate_lease(&lease)?;
    Ok(lease)
}

pub(super) fn encode_v3_lease(lease: &RuntimeLease) -> Result<Vec<u8>, String> {
    validate_lease(lease)?;
    if lease.version != LEASE_VERSION {
        return Err("legacy runtime leases cannot be published".into());
    }
    let owner_mode = lease
        .owner_mode
        .ok_or_else(|| "v3 runtime lease owner mode is missing".to_string())?;
    let wire = RuntimeLeaseV3 {
        version: lease.version,
        owner_mode,
        fingerprint: lease.fingerprint.clone(),
        managed_source: lease.managed_source.clone(),
        owner_pid: lease.owner_pid,
        owner_start_time: lease.owner_start_time,
        child_pid: lease.child_pid,
        child_start_time: lease.child_start_time,
        child_pgid: lease.child_pgid,
        server: lease.server.clone(),
        model_id: lease.model_id.clone(),
        port: lease.port,
    };
    serde_json::to_vec_pretty(&wire).map_err(|error| error.to_string())
}

pub(super) fn encode_lease(lease: &RuntimeLease) -> Result<Vec<u8>, String> {
    validate_lease(lease)?;
    if lease.version == LEASE_VERSION {
        return encode_v3_lease(lease);
    }
    if lease.version != SERVICE_LEASE_VERSION {
        return Err("legacy runtime leases cannot be published".into());
    }
    let service = lease
        .service
        .as_ref()
        .ok_or_else(|| "v4 service runtime lease fields are missing".to_string())?;
    let wire = RuntimeLeaseV4 {
        version: lease.version,
        owner_mode: lease
            .owner_mode
            .ok_or_else(|| "v4 runtime lease owner mode is missing".to_string())?,
        fingerprint: lease.fingerprint.clone(),
        managed_source: lease.managed_source.clone(),
        owner_pid: lease.owner_pid,
        owner_start_time: lease.owner_start_time,
        child_pid: lease.child_pid,
        child_start_time: lease.child_start_time,
        child_pgid: lease.child_pgid,
        server: lease.server.clone(),
        model_id: lease.model_id.clone(),
        port: lease.port,
        endpoint: service.endpoint.clone(),
        parallel: service.parallel,
        offline: service.offline,
    };
    serde_json::to_vec_pretty(&wire).map_err(|error| error.to_string())
}
