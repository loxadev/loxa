pub(super) const CREATE_CONVERSATIONS_V1: &str = r#"CREATE TABLE conversations (
    id BLOB PRIMARY KEY NOT NULL
        CHECK (typeof(id) = 'blob' AND length(id) = 16),
    model_id TEXT NOT NULL
        CHECK (typeof(model_id) = 'text' AND length(CAST(model_id AS BLOB)) BETWEEN 1 AND 120),
    manifest_version INTEGER NOT NULL CHECK (manifest_version BETWEEN 1 AND 3),
    binding_profile INTEGER NOT NULL CHECK (binding_profile IN (0, 1)),
    qualified_profile TEXT,
    qualified_engine TEXT,
    qualified_engine_build TEXT,
    primary_filename TEXT NOT NULL
        CHECK (typeof(primary_filename) = 'text' AND length(CAST(primary_filename AS BLOB)) BETWEEN 1 AND 1024),
    primary_sha256 BLOB NOT NULL
        CHECK (typeof(primary_sha256) = 'blob' AND length(primary_sha256) = 32),
    primary_size INTEGER NOT NULL CHECK (primary_size > 0),
    primary_source_kind INTEGER NOT NULL CHECK (primary_source_kind IN (0, 1)),
    primary_source_repo TEXT,
    primary_source_revision TEXT,
    primary_source_filename TEXT NOT NULL
        CHECK (typeof(primary_source_filename) = 'text' AND length(CAST(primary_source_filename AS BLOB)) BETWEEN 1 AND 1024),
    draft_filename TEXT,
    draft_sha256 BLOB,
    draft_size INTEGER,
    draft_source_kind INTEGER,
    draft_source_repo TEXT,
    draft_source_revision TEXT,
    draft_source_filename TEXT,
    title TEXT NOT NULL
        CHECK (typeof(title) = 'text' AND length(CAST(title AS BLOB)) BETWEEN 1 AND 256),
    system_instruction TEXT NOT NULL
        CHECK (typeof(system_instruction) = 'text' AND length(CAST(system_instruction AS BLOB)) <= 16384),
    max_output_tokens INTEGER NOT NULL CHECK (max_output_tokens BETWEEN 1 AND 2147483647),
    created_ms INTEGER NOT NULL CHECK (created_ms >= 0),
    updated_ms INTEGER NOT NULL CHECK (updated_ms >= created_ms),
    revision INTEGER NOT NULL CHECK (revision >= 1),
    profile_revision INTEGER NOT NULL CHECK (profile_revision >= 1),
    deleted INTEGER NOT NULL CHECK (deleted IN (0, 1)),
    CHECK (
        (primary_source_kind = 0 AND primary_source_repo IS NULL AND primary_source_revision IS NULL)
        OR
        (primary_source_kind = 1 AND primary_source_repo IS NOT NULL AND primary_source_revision IS NOT NULL)
    ),
    CHECK (
        (manifest_version IN (1, 2) AND binding_profile = 0)
        OR (manifest_version = 3 AND binding_profile = 1)
    ),
    CHECK (
        (manifest_version = 1 AND primary_source_kind = 1)
        OR (manifest_version = 2 AND primary_source_kind = 0)
        OR manifest_version = 3
    ),
    CHECK (
        (binding_profile = 0
         AND qualified_profile IS NULL
         AND qualified_engine IS NULL
         AND qualified_engine_build IS NULL)
        OR
        (binding_profile = 1
         AND qualified_profile IS NOT NULL AND typeof(qualified_profile) = 'text'
         AND length(CAST(qualified_profile AS BLOB)) BETWEEN 1 AND 64
         AND qualified_engine IS NOT NULL AND typeof(qualified_engine) = 'text'
         AND length(CAST(qualified_engine AS BLOB)) BETWEEN 1 AND 64
         AND qualified_engine_build IS NOT NULL AND typeof(qualified_engine_build) = 'text'
         AND length(CAST(qualified_engine_build AS BLOB)) BETWEEN 1 AND 64)
    ),
    CHECK (
        primary_source_repo IS NULL
        OR (typeof(primary_source_repo) = 'text'
            AND length(CAST(primary_source_repo AS BLOB)) BETWEEN 1 AND 256)
    ),
    CHECK (
        primary_source_revision IS NULL
        OR (typeof(primary_source_revision) = 'text'
            AND length(CAST(primary_source_revision AS BLOB)) = 40)
    ),
    CHECK (
        (binding_profile = 0
         AND draft_filename IS NULL AND draft_sha256 IS NULL AND draft_size IS NULL
         AND draft_source_kind IS NULL AND draft_source_repo IS NULL
         AND draft_source_revision IS NULL AND draft_source_filename IS NULL)
        OR
        (binding_profile = 1
         AND draft_filename IS NOT NULL AND typeof(draft_filename) = 'text'
         AND length(CAST(draft_filename AS BLOB)) BETWEEN 1 AND 1024
         AND draft_sha256 IS NOT NULL AND typeof(draft_sha256) = 'blob' AND length(draft_sha256) = 32
         AND draft_size IS NOT NULL AND typeof(draft_size) = 'integer' AND draft_size > 0
         AND draft_source_kind IS NOT NULL AND typeof(draft_source_kind) = 'integer'
         AND draft_source_kind IN (0, 1)
         AND draft_source_filename IS NOT NULL AND typeof(draft_source_filename) = 'text'
         AND length(CAST(draft_source_filename AS BLOB)) BETWEEN 1 AND 1024
         AND ((draft_source_kind = 0 AND draft_source_repo IS NULL AND draft_source_revision IS NULL)
              OR (draft_source_kind = 1
                  AND draft_source_repo IS NOT NULL AND typeof(draft_source_repo) = 'text'
                  AND length(CAST(draft_source_repo AS BLOB)) BETWEEN 1 AND 256
                  AND draft_source_revision IS NOT NULL AND typeof(draft_source_revision) = 'text'
                  AND length(CAST(draft_source_revision AS BLOB)) = 40)))
    )
) STRICT, WITHOUT ROWID"#;

pub(super) const CREATE_RECENCY_INDEX_V1: &str = r#"CREATE INDEX conversations_recency
    ON conversations (deleted, updated_ms DESC, id DESC)"#;

pub(super) const CREATE_TURNS_V2: &str = r#"CREATE TABLE turns (
    id BLOB PRIMARY KEY NOT NULL
        CHECK (typeof(id) = 'blob' AND length(id) = 16),
    conversation_id BLOB NOT NULL REFERENCES conversations(id),
    ordinal INTEGER NOT NULL CHECK (ordinal > 0),
    user_text TEXT NOT NULL
        CHECK (typeof(user_text) = 'text' AND length(CAST(user_text AS BLOB)) BETWEEN 1 AND 32768),
    selected_attempt_id BLOB
        REFERENCES attempts(id) DEFERRABLE INITIALLY DEFERRED,
    UNIQUE (conversation_id, ordinal)
) STRICT, WITHOUT ROWID"#;

pub(super) const CREATE_ATTEMPTS_V2: &str = r#"CREATE TABLE attempts (
    id BLOB PRIMARY KEY NOT NULL
        CHECK (typeof(id) = 'blob' AND length(id) = 16),
    turn_id BLOB NOT NULL REFERENCES turns(id),
    attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
    submission_id BLOB NOT NULL UNIQUE
        CHECK (typeof(submission_id) = 'blob' AND length(submission_id) = 16),
    submission_hash BLOB NOT NULL
        CHECK (typeof(submission_hash) = 'blob' AND length(submission_hash) = 32),
    admitted_conversation_revision INTEGER NOT NULL CHECK (admitted_conversation_revision > 0),
    admitted_profile_revision INTEGER NOT NULL CHECK (admitted_profile_revision > 0),
    prior_attempt_id BLOB REFERENCES attempts(id),
    owner_epoch TEXT NOT NULL
        CHECK (typeof(owner_epoch) = 'text' AND length(CAST(owner_epoch AS BLOB)) BETWEEN 1 AND 128),
    operation_generation INTEGER NOT NULL CHECK (operation_generation > 0),
    model_id TEXT NOT NULL
        CHECK (typeof(model_id) = 'text' AND length(CAST(model_id AS BLOB)) BETWEEN 1 AND 120),
    applied_engine_build TEXT NOT NULL
        CHECK (typeof(applied_engine_build) = 'text' AND length(CAST(applied_engine_build AS BLOB)) BETWEEN 1 AND 64),
    applied_engine_version TEXT NOT NULL
        CHECK (typeof(applied_engine_version) = 'text' AND length(CAST(applied_engine_version AS BLOB)) BETWEEN 1 AND 128),
    runtime_fingerprint BLOB NOT NULL
        CHECK (typeof(runtime_fingerprint) = 'blob' AND length(runtime_fingerprint) BETWEEN 1 AND 131072),
    effective_context INTEGER NOT NULL CHECK (effective_context BETWEEN 1 AND 2147483647),
    system_instruction TEXT NOT NULL
        CHECK (typeof(system_instruction) = 'text' AND length(CAST(system_instruction AS BLOB)) <= 16384),
    max_output_tokens INTEGER NOT NULL CHECK (max_output_tokens BETWEEN 1 AND 2147483647),
    prompt_basis BLOB NOT NULL
        CHECK (typeof(prompt_basis) = 'blob' AND length(prompt_basis) BETWEEN 1 AND 131072),
    execution_outcome INTEGER NOT NULL CHECK (execution_outcome BETWEEN 0 AND 4),
    save_outcome INTEGER NOT NULL CHECK (save_outcome BETWEEN 0 AND 3),
    saved_end INTEGER NOT NULL CHECK (saved_end BETWEEN 0 AND 16777216),
    generated_end INTEGER CHECK (generated_end BETWEEN 0 AND 16777216),
    terminal_saved_end INTEGER CHECK (terminal_saved_end BETWEEN 0 AND 16777216),
    failure_code TEXT
        CHECK (failure_code IS NULL OR (typeof(failure_code) = 'text'
               AND length(CAST(failure_code AS BLOB)) BETWEEN 1 AND 64)),
    created_ms INTEGER NOT NULL CHECK (created_ms >= 0),
    updated_ms INTEGER NOT NULL CHECK (updated_ms >= created_ms),
    UNIQUE (turn_id, attempt_number),
    CHECK (
        (save_outcome = 0 AND terminal_saved_end IS NULL)
        OR (save_outcome = 1 AND generated_end IS NOT NULL AND terminal_saved_end IS NOT NULL
            AND terminal_saved_end = generated_end AND saved_end = generated_end)
        OR (save_outcome IN (2, 3) AND terminal_saved_end IS NULL)
    )
) STRICT, WITHOUT ROWID"#;

pub(super) const CREATE_DRAFTS_V2: &str = r#"CREATE TABLE drafts (
    id BLOB PRIMARY KEY NOT NULL
        CHECK (typeof(id) = 'blob' AND length(id) = 16),
    desktop_client_id BLOB NOT NULL
        CHECK (typeof(desktop_client_id) = 'blob' AND length(desktop_client_id) = 16),
    conversation_id BLOB REFERENCES conversations(id),
    revision INTEGER NOT NULL CHECK (revision >= 0),
    consumed_revision INTEGER NOT NULL CHECK (consumed_revision >= 0 AND consumed_revision <= revision),
    text TEXT NOT NULL
        CHECK (typeof(text) = 'text' AND length(CAST(text AS BLOB)) <= 32768),
    text_hash BLOB NOT NULL
        CHECK (typeof(text_hash) = 'blob' AND length(text_hash) = 32),
    updated_ms INTEGER NOT NULL CHECK (updated_ms >= 0)
) STRICT, WITHOUT ROWID"#;

pub(super) const CREATE_ATTEMPTS_UNRESOLVED_V2: &str = r#"CREATE INDEX attempts_unresolved
    ON attempts (owner_epoch, execution_outcome, save_outcome)"#;
pub(super) const CREATE_ATTEMPTS_LATEST_V2: &str = r#"CREATE INDEX attempts_latest
    ON attempts (turn_id, attempt_number DESC)"#;
pub(super) const CREATE_DRAFTS_CLIENT_CONVERSATION_V2: &str = r#"CREATE INDEX drafts_client_conversation
    ON drafts (desktop_client_id, conversation_id, updated_ms DESC)"#;
pub(super) const CREATE_DRAFTS_CLIENT_UNBOUND_V2: &str = r#"CREATE INDEX drafts_client_unbound
    ON drafts (desktop_client_id, updated_ms DESC) WHERE conversation_id IS NULL"#;

pub(super) const CREATE_ATTEMPT_CHUNKS_V3: &str = r#"CREATE TABLE attempt_chunks (
    attempt_id BLOB NOT NULL REFERENCES attempts(id),
    start_offset INTEGER NOT NULL CHECK (start_offset BETWEEN 0 AND 16777215),
    end_offset INTEGER NOT NULL CHECK (end_offset BETWEEN 1 AND 16777216),
    content TEXT NOT NULL
        CHECK (typeof(content) = 'text'
               AND length(CAST(content AS BLOB)) BETWEEN 1 AND 65536),
    PRIMARY KEY (attempt_id, start_offset),
    CHECK (end_offset = start_offset + length(CAST(content AS BLOB)))
) STRICT, WITHOUT ROWID"#;

pub(super) const CREATE_ATTEMPTS_RECOVERY_V3: &str = r#"CREATE INDEX attempts_recovery
    ON attempts (id) WHERE execution_outcome = 0 OR save_outcome IN (0, 2)"#;

pub(super) const CREATE_ATTEMPT_FINALIZATIONS_V3: &str = r#"CREATE TABLE attempt_finalizations (
    attempt_id BLOB PRIMARY KEY NOT NULL REFERENCES attempts(id),
    start_offset INTEGER NOT NULL CHECK (start_offset BETWEEN 0 AND 16777216),
    end_offset INTEGER NOT NULL CHECK (end_offset BETWEEN start_offset AND 16777216)
) STRICT, WITHOUT ROWID"#;

pub(super) const CREATE_TURNS_CONVERSATION_V3: &str = r#"CREATE INDEX turns_conversation
    ON turns (conversation_id, id)"#;
pub(super) const CREATE_TURNS_SELECTED_ATTEMPT_V3: &str = r#"CREATE INDEX turns_selected_attempt
    ON turns (selected_attempt_id) WHERE selected_attempt_id IS NOT NULL"#;
pub(super) const CREATE_ATTEMPTS_PRIOR_V3: &str = r#"CREATE INDEX attempts_prior
    ON attempts (prior_attempt_id) WHERE prior_attempt_id IS NOT NULL"#;
pub(super) const CREATE_DRAFTS_CONVERSATION_V3: &str = r#"CREATE INDEX drafts_conversation
    ON drafts (conversation_id, id) WHERE conversation_id IS NOT NULL"#;
