mod attachment;
mod lease;
mod lock;
mod observation;
mod ownership;
mod process;
mod record;
mod recovery;

pub(crate) use attachment::{
    lookup_persistent_runtime, lookup_runtime_presence, AttachedRuntime, PersistentRuntimeLookup,
    RuntimePresence,
};
pub(crate) use observation::{
    ForegroundObservation, ForegroundObserver, RuntimeOwner, RuntimeProvenance,
};
pub(crate) use ownership::{
    RuntimeChildOwnership, RuntimeLeasePublication, RuntimeOwnership, RuntimeOwnershipAcquireError,
};
pub(crate) use process::{
    current_process_start_identity, terminate_process_group, terminate_process_group_immediately,
    terminate_stale_process_group,
};
pub(crate) use record::clear_terminated_owned_lease;
pub(crate) use recovery::recover_stale;

#[cfg(test)]
use crate::runtime_fingerprint::RuntimeFingerprint;
#[cfg(test)]
use lease::{
    decode_lease, encode_lease, encode_v3_lease, LeaseOwnerMode, RuntimeLease, RuntimeLeaseV1,
    RuntimeLeaseV2, ServiceLeaseFields, LEASE_VERSION, LEGACY_LEASE_VERSION,
    PERSISTENT_LEASE_VERSION,
};
#[cfg(test)]
use lock::{foreground_lock_is_held, foreground_lock_is_held_with_after_open, open_lock};
#[cfg(all(test, unix))]
use lock::{
    set_foreground_lock_with_command, try_acquire_foreground_lock, LocalForegroundLock,
    LOCAL_FOREGROUND_LOCK_OPERATIONS,
};
#[cfg(test)]
use process::{command_has_unique_option, process_group, process_snapshot};
#[cfg(test)]
pub(crate) use process::{
    fail_next_owned_group_terminations_for_test, process_group_has_live_members,
};
#[cfg(test)]
use record::{read_lease, read_regular_file, write_lease, MAX_RUNTIME_RECORD_BYTES};
#[cfg(all(test, target_os = "macos"))]
pub(crate) use recovery::stages::fail_next_execution_stage_termination_for_test;
#[cfg(all(test, target_os = "macos"))]
// Preserve the existing facade path for the test fault's return type.
#[allow(unused_imports)]
pub(crate) use recovery::stages::ExecutionStageTerminationFaultReset;
#[cfg(test)]
use std::ffi::{OsStr, OsString};
#[cfg(test)]
use std::fs;
#[cfg(test)]
use std::path::{Path, PathBuf};

#[cfg(test)]
mod tests;
