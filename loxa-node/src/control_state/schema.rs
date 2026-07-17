use sha2::{Digest, Sha256};

pub(super) const SCHEMA_VERSION: i64 = 1;
pub(super) const MIGRATION_NAME: &str = "capacity_one_control_state";

pub(super) const SCHEMA_V1: &str = r#"
CREATE TABLE loxa_schema_migrations (
  version INTEGER PRIMARY KEY CHECK(version = 1),
  name TEXT NOT NULL,
  checksum TEXT NOT NULL,
  applied_at_ms INTEGER NOT NULL CHECK(applied_at_ms >= 0)
) STRICT;

CREATE TABLE control_meta (
  singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
  node_id TEXT NOT NULL UNIQUE CHECK(length(node_id) = 36),
  slot_id TEXT NOT NULL UNIQUE CHECK(length(slot_id) = 36),
  stream_epoch TEXT NOT NULL UNIQUE CHECK(length(stream_epoch) = 36),
  revision TEXT NOT NULL CHECK(length(revision) BETWEEN 1 AND 20 AND revision NOT GLOB '*[^0-9]*' AND revision NOT LIKE '0%'),
  cursor TEXT NOT NULL CHECK(length(cursor) BETWEEN 1 AND 20 AND cursor NOT GLOB '*[^0-9]*' AND cursor NOT LIKE '0%'),
  schema_version INTEGER NOT NULL CHECK(schema_version = 1),
  migration_source TEXT NOT NULL,
  last_committed_at_unix_ms TEXT NOT NULL CHECK(length(last_committed_at_unix_ms) BETWEEN 1 AND 20 AND last_committed_at_unix_ms NOT GLOB '*[^0-9]*' AND (last_committed_at_unix_ms = '0' OR last_committed_at_unix_ms NOT LIKE '0%'))
) STRICT;

CREATE TABLE node_state (
  singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
  node_id TEXT NOT NULL UNIQUE CHECK(length(node_id) = 36),
  node_instance_id TEXT CHECK(node_instance_id IS NULL OR length(node_instance_id) = 36),
  control_endpoint TEXT CHECK(control_endpoint IS NULL OR length(CAST(control_endpoint AS BLOB)) <= 256),
  status TEXT NOT NULL CHECK(status IN ('unpublished', 'running', 'stopping', 'recovery')),
  model_download INTEGER NOT NULL CHECK(model_download IN (0, 1)),
  slot_load INTEGER NOT NULL CHECK(slot_load IN (0, 1)),
  slot_unload INTEGER NOT NULL CHECK(slot_unload IN (0, 1)),
  operation_cancel INTEGER NOT NULL CHECK(operation_cancel IN (0, 1)),
  operation_stream INTEGER NOT NULL CHECK(operation_stream IN (0, 1)),
  FOREIGN KEY(singleton) REFERENCES control_meta(singleton),
  FOREIGN KEY(node_id) REFERENCES control_meta(node_id)
) STRICT;

CREATE TABLE slot_state (
  singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
  slot_id TEXT NOT NULL UNIQUE CHECK(length(slot_id) = 36),
  name TEXT NOT NULL CHECK(name = 'default'),
  status TEXT NOT NULL CHECK(status IN ('unloaded', 'loading', 'ready', 'unloading', 'recovery')),
  model_id TEXT CHECK(model_id IS NULL OR length(CAST(model_id AS BLOB)) BETWEEN 1 AND 256),
  operation_id TEXT CHECK(operation_id IS NULL OR length(operation_id) = 36),
  error_code TEXT CHECK(error_code IS NULL OR error_code = 'lifecycle_recovery_required'),
  error_message TEXT CHECK(error_message IS NULL OR length(CAST(error_message AS BLOB)) BETWEEN 1 AND 256),
  updated_revision TEXT NOT NULL CHECK(length(updated_revision) BETWEEN 1 AND 20 AND updated_revision NOT GLOB '*[^0-9]*' AND updated_revision NOT LIKE '0%'),
  updated_at_unix_ms TEXT NOT NULL CHECK(length(updated_at_unix_ms) BETWEEN 1 AND 20 AND updated_at_unix_ms NOT GLOB '*[^0-9]*' AND (updated_at_unix_ms = '0' OR updated_at_unix_ms NOT LIKE '0%')),
  CHECK(
    (status = 'unloaded' AND model_id IS NULL AND operation_id IS NULL) OR
    (status = 'loading' AND operation_id IS NOT NULL) OR
    (status = 'ready' AND model_id IS NOT NULL AND operation_id IS NULL) OR
    (status = 'unloading' AND model_id IS NOT NULL AND operation_id IS NOT NULL) OR
    (status = 'recovery' AND operation_id IS NULL AND error_code = 'lifecycle_recovery_required' AND error_message IS NOT NULL)
  ),
  CHECK(status = 'recovery' OR (error_code IS NULL AND error_message IS NULL)),
  FOREIGN KEY(singleton) REFERENCES control_meta(singleton),
  FOREIGN KEY(slot_id) REFERENCES control_meta(slot_id)
) STRICT;

CREATE TABLE operations (
  operation_id TEXT PRIMARY KEY CHECK(length(operation_id) = 36),
  slot_id TEXT NOT NULL CHECK(length(slot_id) = 36),
  admitting_node_instance_id TEXT NOT NULL CHECK(length(admitting_node_instance_id) = 36),
  v1_ordinal INTEGER CHECK(v1_ordinal IS NULL OR v1_ordinal >= 1),
  kind TEXT NOT NULL CHECK(kind IN ('download', 'load', 'unload')),
  status TEXT NOT NULL CHECK(status IN ('queued', 'running', 'cancelling', 'succeeded', 'failed', 'cancelled')),
  model_id TEXT CHECK(model_id IS NULL OR length(CAST(model_id AS BLOB)) BETWEEN 1 AND 256),
  progress_current TEXT,
  progress_total TEXT,
  error_code TEXT CHECK(error_code IS NULL OR length(CAST(error_code AS BLOB)) BETWEEN 1 AND 64),
  error_message TEXT CHECK(error_message IS NULL OR length(CAST(error_message AS BLOB)) BETWEEN 1 AND 256),
  created_revision TEXT NOT NULL CHECK(length(created_revision) BETWEEN 1 AND 20 AND created_revision NOT GLOB '*[^0-9]*' AND created_revision NOT LIKE '0%'),
  updated_revision TEXT NOT NULL CHECK(length(updated_revision) BETWEEN 1 AND 20 AND updated_revision NOT GLOB '*[^0-9]*' AND updated_revision NOT LIKE '0%'),
  created_at_unix_ms TEXT NOT NULL CHECK(length(created_at_unix_ms) BETWEEN 1 AND 20 AND created_at_unix_ms NOT GLOB '*[^0-9]*' AND (created_at_unix_ms = '0' OR created_at_unix_ms NOT LIKE '0%')),
  updated_at_unix_ms TEXT NOT NULL CHECK(length(updated_at_unix_ms) BETWEEN 1 AND 20 AND updated_at_unix_ms NOT GLOB '*[^0-9]*' AND (updated_at_unix_ms = '0' OR updated_at_unix_ms NOT LIKE '0%')),
  CHECK(progress_current IS NULL OR (length(progress_current) BETWEEN 1 AND 20 AND progress_current NOT GLOB '*[^0-9]*' AND (progress_current = '0' OR progress_current NOT LIKE '0%'))),
  CHECK(progress_total IS NULL OR (length(progress_total) BETWEEN 1 AND 20 AND progress_total NOT GLOB '*[^0-9]*' AND (progress_total = '0' OR progress_total NOT LIKE '0%'))),
  CHECK(progress_current IS NOT NULL OR progress_total IS NULL),
  CHECK((status = 'failed') = (error_code IS NOT NULL AND error_message IS NOT NULL)),
  CHECK(length(updated_revision) > length(created_revision) OR (length(updated_revision) = length(created_revision) AND updated_revision >= created_revision)),
  CHECK(length(updated_at_unix_ms) > length(created_at_unix_ms) OR (length(updated_at_unix_ms) = length(created_at_unix_ms) AND updated_at_unix_ms >= created_at_unix_ms)),
  FOREIGN KEY(slot_id) REFERENCES slot_state(slot_id),
  UNIQUE(admitting_node_instance_id, v1_ordinal)
) STRICT;

CREATE UNIQUE INDEX one_active_lifecycle_operation
ON operations(1)
WHERE kind IN ('load', 'unload') AND status IN ('queued', 'running', 'cancelling');

CREATE TABLE events (
  event_id TEXT PRIMARY KEY CHECK(length(event_id) = 36),
  stream_epoch TEXT NOT NULL CHECK(length(stream_epoch) = 36),
  sequence TEXT NOT NULL CHECK(length(sequence) BETWEEN 1 AND 20 AND sequence NOT GLOB '*[^0-9]*' AND sequence NOT LIKE '0%'),
  revision TEXT NOT NULL CHECK(length(revision) BETWEEN 1 AND 20 AND revision NOT GLOB '*[^0-9]*' AND revision NOT LIKE '0%'),
  node_instance_id TEXT CHECK(node_instance_id IS NULL OR length(node_instance_id) = 36),
  v1_sequence INTEGER CHECK(v1_sequence IS NULL OR v1_sequence >= 1),
  event_kind TEXT NOT NULL CHECK(event_kind IN ('initialized', 'node_changed', 'slot_changed', 'operation_changed')),
  payload_json TEXT NOT NULL CHECK(length(CAST(payload_json AS BLOB)) <= 16384),
  FOREIGN KEY(stream_epoch) REFERENCES control_meta(stream_epoch),
  UNIQUE(stream_epoch, sequence),
  UNIQUE(node_instance_id, v1_sequence)
) STRICT;
"#;

pub(super) fn schema_checksum() -> String {
    let digest = Sha256::digest(SCHEMA_V1.as_bytes());
    let mut checksum = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write;
        let _ = write!(&mut checksum, "{byte:02x}");
    }
    checksum
}

#[cfg(test)]
pub(super) const FORMER_INTERMEDIATE_TASK3A_SCHEMA_V1: &str = r#"
CREATE TABLE loxa_schema_migrations (version INTEGER PRIMARY KEY CHECK(version = 1), name TEXT NOT NULL, checksum TEXT NOT NULL, applied_at_ms INTEGER NOT NULL CHECK(applied_at_ms >= 0)) STRICT;
CREATE TABLE control_meta (singleton INTEGER PRIMARY KEY CHECK(singleton = 1), node_id TEXT NOT NULL UNIQUE CHECK(length(node_id) = 36), slot_id TEXT NOT NULL UNIQUE CHECK(length(slot_id) = 36), stream_epoch TEXT NOT NULL UNIQUE CHECK(length(stream_epoch) = 36), revision TEXT NOT NULL CHECK(length(revision) BETWEEN 1 AND 20 AND revision NOT GLOB '*[^0-9]*' AND revision NOT LIKE '0%'), cursor TEXT NOT NULL CHECK(length(cursor) BETWEEN 1 AND 20 AND cursor NOT GLOB '*[^0-9]*' AND cursor NOT LIKE '0%'), schema_version INTEGER NOT NULL CHECK(schema_version = 1), migration_source TEXT NOT NULL, last_committed_at_unix_ms TEXT NOT NULL CHECK(length(last_committed_at_unix_ms) BETWEEN 1 AND 20 AND last_committed_at_unix_ms NOT GLOB '*[^0-9]*' AND (last_committed_at_unix_ms = '0' OR last_committed_at_unix_ms NOT LIKE '0%'))) STRICT;
CREATE TABLE node_state (singleton INTEGER PRIMARY KEY CHECK(singleton = 1), node_id TEXT NOT NULL UNIQUE CHECK(length(node_id) = 36), node_instance_id TEXT CHECK(node_instance_id IS NULL OR length(node_instance_id) = 36), control_endpoint TEXT CHECK(control_endpoint IS NULL OR length(CAST(control_endpoint AS BLOB)) <= 256), status TEXT NOT NULL CHECK(status IN ('unpublished', 'running', 'stopping', 'recovery')), can_load INTEGER NOT NULL CHECK(can_load IN (0, 1)), can_unload INTEGER NOT NULL CHECK(can_unload IN (0, 1)), can_download INTEGER NOT NULL CHECK(can_download IN (0, 1)), FOREIGN KEY(singleton) REFERENCES control_meta(singleton), FOREIGN KEY(node_id) REFERENCES control_meta(node_id)) STRICT;
CREATE TABLE slot_state (singleton INTEGER PRIMARY KEY CHECK(singleton = 1), slot_id TEXT NOT NULL UNIQUE CHECK(length(slot_id) = 36), name TEXT NOT NULL CHECK(name = 'default'), status TEXT NOT NULL CHECK(status IN ('unloaded', 'loading', 'ready', 'unloading', 'recovery')), model_id TEXT CHECK(model_id IS NULL OR length(CAST(model_id AS BLOB)) BETWEEN 1 AND 256), operation_id TEXT CHECK(operation_id IS NULL OR length(operation_id) = 36), updated_revision TEXT NOT NULL CHECK(length(updated_revision) BETWEEN 1 AND 20 AND updated_revision NOT GLOB '*[^0-9]*' AND updated_revision NOT LIKE '0%'), updated_at_unix_ms TEXT NOT NULL CHECK(length(updated_at_unix_ms) BETWEEN 1 AND 20 AND updated_at_unix_ms NOT GLOB '*[^0-9]*' AND (updated_at_unix_ms = '0' OR updated_at_unix_ms NOT LIKE '0%')), CHECK((status = 'ready' AND model_id IS NOT NULL) OR (status != 'ready')), CHECK((status = 'unloaded' AND model_id IS NULL AND operation_id IS NULL) OR status != 'unloaded'), FOREIGN KEY(singleton) REFERENCES control_meta(singleton), FOREIGN KEY(slot_id) REFERENCES control_meta(slot_id)) STRICT;
CREATE TABLE operations (operation_id TEXT PRIMARY KEY CHECK(length(operation_id) = 36), slot_id TEXT NOT NULL CHECK(length(slot_id) = 36), admitting_node_instance_id TEXT NOT NULL CHECK(length(admitting_node_instance_id) = 36), v1_ordinal INTEGER CHECK(v1_ordinal IS NULL OR v1_ordinal >= 1), kind TEXT NOT NULL CHECK(kind IN ('load', 'unload', 'download', 'cancel')), status TEXT NOT NULL CHECK(status IN ('queued', 'running', 'cancelling', 'succeeded', 'failed', 'cancelled')), model_id TEXT CHECK(model_id IS NULL OR length(CAST(model_id AS BLOB)) BETWEEN 1 AND 256), progress_current TEXT, progress_total TEXT, error_code TEXT CHECK(error_code IS NULL OR length(CAST(error_code AS BLOB)) BETWEEN 1 AND 64), error_message TEXT CHECK(error_message IS NULL OR length(CAST(error_message AS BLOB)) BETWEEN 1 AND 256), created_revision TEXT NOT NULL CHECK(length(created_revision) BETWEEN 1 AND 20 AND created_revision NOT GLOB '*[^0-9]*' AND created_revision NOT LIKE '0%'), updated_revision TEXT NOT NULL CHECK(length(updated_revision) BETWEEN 1 AND 20 AND updated_revision NOT GLOB '*[^0-9]*' AND updated_revision NOT LIKE '0%'), created_at_unix_ms TEXT NOT NULL CHECK(length(created_at_unix_ms) BETWEEN 1 AND 20 AND created_at_unix_ms NOT GLOB '*[^0-9]*' AND (created_at_unix_ms = '0' OR created_at_unix_ms NOT LIKE '0%')), updated_at_unix_ms TEXT NOT NULL CHECK(length(updated_at_unix_ms) BETWEEN 1 AND 20 AND updated_at_unix_ms NOT GLOB '*[^0-9]*' AND (updated_at_unix_ms = '0' OR updated_at_unix_ms NOT LIKE '0%')), CHECK((progress_current IS NULL) = (progress_total IS NULL)), CHECK((status = 'failed') = (error_code IS NOT NULL AND error_message IS NOT NULL)), CHECK(length(updated_revision) > length(created_revision) OR (length(updated_revision) = length(created_revision) AND updated_revision >= created_revision)), CHECK(length(updated_at_unix_ms) > length(created_at_unix_ms) OR (length(updated_at_unix_ms) = length(created_at_unix_ms) AND updated_at_unix_ms >= created_at_unix_ms)), FOREIGN KEY(slot_id) REFERENCES slot_state(slot_id), UNIQUE(admitting_node_instance_id, v1_ordinal)) STRICT;
CREATE UNIQUE INDEX one_active_lifecycle_operation ON operations(1) WHERE status IN ('queued', 'running', 'cancelling');
CREATE TABLE events (event_id TEXT PRIMARY KEY CHECK(length(event_id) = 36), stream_epoch TEXT NOT NULL CHECK(length(stream_epoch) = 36), sequence TEXT NOT NULL CHECK(length(sequence) BETWEEN 1 AND 20 AND sequence NOT GLOB '*[^0-9]*' AND sequence NOT LIKE '0%'), revision TEXT NOT NULL CHECK(length(revision) BETWEEN 1 AND 20 AND revision NOT GLOB '*[^0-9]*' AND revision NOT LIKE '0%'), node_instance_id TEXT CHECK(node_instance_id IS NULL OR length(node_instance_id) = 36), v1_sequence INTEGER CHECK(v1_sequence IS NULL OR v1_sequence >= 1), event_kind TEXT NOT NULL CHECK(event_kind IN ('initialized', 'node_changed', 'slot_changed', 'operation_changed')), payload_json TEXT NOT NULL CHECK(length(CAST(payload_json AS BLOB)) <= 16384), FOREIGN KEY(stream_epoch) REFERENCES control_meta(stream_epoch), UNIQUE(stream_epoch, sequence), UNIQUE(node_instance_id, v1_sequence)) STRICT;
"#;
