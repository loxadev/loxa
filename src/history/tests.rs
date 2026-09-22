use super::admission::{DraftSubmission, PromptReference};
use super::*;
use crate::catalog::{
    Artifact, ArtifactProvenance, ArtifactRole, Manifest, Origin, RuntimeQualification,
    GEMMA4_DRAFT_SHA256, GEMMA4_DRAFT_SIZE, GEMMA4_LLAMA_BUILD, GEMMA4_MODEL_SHA256,
    GEMMA4_MODEL_SIZE, GEMMA4_MTP_PROFILE,
};
use crate::runtime_identity::RuntimeIdentity;
use loxa_ipc::{
    DraftCommand, DraftReply, EffectiveSamplingSettings, GenerationSettings,
    GenerationSettingsPatch, HistoryCommand as WireCommand, HistoryPhase, HistoryReply,
    OptionalSamplingValuePatch, SamplingValue, ServiceSettingsCommand,
};
use rusqlite::{params, Connection};
use sha2::{Digest, Sha256};
use std::fs;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

mod reopen;

fn private_root(label: &str) -> (tempfile::TempDir, PathBuf) {
    let directory = tempfile::Builder::new()
        .prefix(label)
        .tempdir_in("/tmp")
        .unwrap();
    fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let root = fs::canonicalize(directory.path()).unwrap();
    (directory, root)
}

fn install_local_manifest(models: &Path, id: &str) {
    let model_dir = models.join(id);
    fs::create_dir_all(&model_dir).unwrap();
    fs::write(model_dir.join("model.gguf"), b"GGUF").unwrap();
    let manifest = Manifest {
        version: 2,
        id: id.into(),
        repo: None,
        revision: None,
        remote_filename: None,
        origin: Some(Origin::Local),
        source_filename: Some("source.gguf".into()),
        local_filename: "model.gguf".into(),
        sha256: "b83633aa785344791618f2fddf131b010ea04912a60430760b070bad293f65bd".into(),
        size: 4,
        artifacts: None,
        profile: None,
        runtime: None,
    };
    crate::catalog::publish_manifest(models, &manifest).unwrap();
}

fn create_local_conversation(
    connection: &mut Connection,
    models: &Path,
) -> loxa_ipc::ConversationSummary {
    match conversations::execute(
        connection,
        models,
        RuntimeIdentity::BundledB10344,
        WireCommand::CreateConversation {
            model_id: "demo".into(),
        },
    )
    .unwrap()
    {
        HistoryReply::Conversation(conversation) => conversation,
        _ => panic!("create returned the wrong history reply"),
    }
}

fn generic_service_fingerprint() -> crate::runtime_fingerprint::RuntimeFingerprint {
    let manifest = Manifest {
        version: 2,
        id: "demo".into(),
        repo: None,
        revision: None,
        remote_filename: None,
        origin: Some(Origin::Local),
        source_filename: Some("source.gguf".into()),
        local_filename: "model.gguf".into(),
        sha256: "b83633aa785344791618f2fddf131b010ea04912a60430760b070bad293f65bd".into(),
        size: 4,
        artifacts: None,
        profile: None,
        runtime: None,
    };
    crate::runtime_fingerprint::RuntimeFingerprint::from_manifest_for_service(
        &manifest,
        4096,
        crate::runtime_fingerprint::EffectiveProfile::Generic,
    )
    .unwrap()
}

fn effective_sampling() -> EffectiveSamplingSettings {
    EffectiveSamplingSettings {
        temperature: SamplingValue::new(0.8).unwrap(),
        top_p: SamplingValue::new(0.95).unwrap(),
    }
}

fn prepared_admission(
    conversation_id: [u8; 16],
    revision: i64,
    submission: u8,
    hash: u8,
    generation: i64,
    kind: AdmissionKind,
) -> PreparedAdmission {
    prepared_admission_with_basis(
        conversation_id,
        revision,
        submission,
        hash,
        generation,
        kind,
        PromptBasis {
            references: Vec::new(),
        },
    )
}

fn prepared_admission_with_basis(
    conversation_id: [u8; 16],
    revision: i64,
    submission: u8,
    hash: u8,
    generation: i64,
    kind: AdmissionKind,
    prompt_basis: PromptBasis,
) -> PreparedAdmission {
    PreparedAdmission::new(
        conversation_id,
        [submission; 16],
        [hash; 32],
        revision,
        1,
        "boot-1".into(),
        generation,
        Arc::new(generic_service_fingerprint()),
        RuntimeIdentity::BundledB10344,
        4096,
        String::new(),
        512,
        effective_sampling(),
        prompt_basis,
        kind,
    )
    .unwrap()
}

fn admitted_attempt(
    connection: &mut Connection,
    models: &Path,
    submission: u8,
    generation: i64,
) -> (loxa_ipc::ConversationSummary, CommittedAdmission) {
    let conversation = create_local_conversation(connection, models);
    let committed = admission::admit_send(
        connection,
        &prepared_admission(
            identity::decode_id(&conversation.id).unwrap(),
            1,
            submission,
            submission,
            generation,
            AdmissionKind::Send {
                user_text: "question".into(),
                draft: None,
            },
        ),
    )
    .unwrap();
    (conversation, committed)
}

fn suffix_input(
    committed: &CommittedAdmission,
    generation: i64,
    start: u64,
    content: &str,
) -> SuffixInput {
    SuffixInput {
        attempt_id: committed.attempt_id,
        owner_epoch: "boot-1".into(),
        operation_generation: generation,
        expected_saved_end: start,
        content: content.into(),
    }
}

fn open_store(root: &Path) -> Result<(Connection, schema::StoreInfo), HistoryError> {
    schema::open_store(
        root,
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(true)),
    )
    .map_err(schema::StoreOpenError::close_for_test)
}

struct StoredBundleBinding {
    profile_kind: i64,
    profile: String,
    engine: String,
    engine_build: String,
    primary_sha256: Vec<u8>,
    draft_sha256: Vec<u8>,
    primary_source_kind: i64,
    draft_source_repo: Option<String>,
}

fn install_bundle_manifest(models: &Path) {
    let model_dir = models.join("gemma4");
    fs::create_dir_all(&model_dir).unwrap();
    let manifest = Manifest {
        version: 3,
        id: "gemma4".into(),
        repo: None,
        revision: None,
        remote_filename: None,
        origin: None,
        source_filename: None,
        local_filename: "model.gguf".into(),
        sha256: GEMMA4_MODEL_SHA256.into(),
        size: GEMMA4_MODEL_SIZE,
        artifacts: Some(vec![
            Artifact {
                role: ArtifactRole::Model,
                local_filename: "model.gguf".into(),
                sha256: GEMMA4_MODEL_SHA256.into(),
                size: GEMMA4_MODEL_SIZE,
                provenance: ArtifactProvenance::HuggingFace {
                    repo: "google/gemma-4".into(),
                    revision: "0123456789abcdef0123456789abcdef01234567".into(),
                    remote_filename: "gemma-4.gguf".into(),
                },
            },
            Artifact {
                role: ArtifactRole::Draft,
                local_filename: "draft.gguf".into(),
                sha256: GEMMA4_DRAFT_SHA256.into(),
                size: GEMMA4_DRAFT_SIZE,
                provenance: ArtifactProvenance::Local {
                    source_filename: "mtp-gemma-4.gguf".into(),
                },
            },
        ]),
        profile: Some(GEMMA4_MTP_PROFILE.into()),
        runtime: Some(RuntimeQualification {
            engine: "llama.cpp".into(),
            build: GEMMA4_LLAMA_BUILD.into(),
        }),
    };
    manifest.validate().unwrap();
    fs::write(
        model_dir.join("manifest.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
}

#[test]
fn store_initialization_is_private_verified_and_reopenable() {
    let (_directory, root) = private_root("loxa-history-schema-");
    let (connection, info) = open_store(&root).unwrap();
    assert_eq!(info.schema_version, 5);
    assert!(!info.sqlite_version.is_empty());
    assert!(!info.sqlite_source_id.is_empty());
    assert_eq!(
        connection
            .query_row::<i64, _, _>("PRAGMA application_id", [], |row| row.get(0))
            .unwrap(),
        schema::application_id()
    );
    let journal: String = connection
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .unwrap();
    assert_eq!(journal.to_ascii_lowercase(), "wal");
    for (pragma, expected) in [
        ("synchronous", 2_i64),
        ("foreign_keys", 1),
        ("trusted_schema", 0),
        ("mmap_size", 0),
        ("wal_autocheckpoint", 256),
    ] {
        let value: i64 = connection
            .query_row(&format!("PRAGMA {pragma}"), [], |row| row.get(0))
            .unwrap();
        assert_eq!(value, expected);
    }
    #[cfg(target_os = "macos")]
    assert_eq!(
        connection
            .query_row::<i64, _, _>("PRAGMA fullfsync", [], |row| row.get(0))
            .unwrap(),
        1
    );
    for filename in ["app.sqlite", "app.sqlite-wal", "app.sqlite-shm"] {
        let metadata = fs::symlink_metadata(root.join(filename)).unwrap();
        assert!(metadata.file_type().is_file());
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
    }
    connection.close().unwrap();

    let metadata = fs::symlink_metadata(root.join("app.sqlite")).unwrap();
    assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
    let (reopened, reopened_info) = open_store(&root).unwrap();
    assert_eq!(reopened_info.schema_version, info.schema_version);
    reopened.close().unwrap();
}

#[test]
fn foreign_newer_malformed_and_unsafe_stores_fail_closed() {
    let (_empty_directory, empty_root) = private_root("loxa-history-empty-");
    fs::write(empty_root.join("app.sqlite"), []).unwrap();
    fs::set_permissions(
        empty_root.join("app.sqlite"),
        fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    assert_eq!(
        open_store(&empty_root).unwrap_err().kind(),
        HistoryErrorKind::UnsupportedSchema
    );
    assert_eq!(
        fs::metadata(empty_root.join("app.sqlite")).unwrap().len(),
        0
    );

    let (_foreign_directory, foreign_root) = private_root("loxa-history-foreign-");
    let foreign = Connection::open(foreign_root.join("app.sqlite")).unwrap();
    foreign
        .execute_batch("PRAGMA application_id = 1; PRAGMA user_version = 1;")
        .unwrap();
    foreign.close().unwrap();
    fs::set_permissions(
        foreign_root.join("app.sqlite"),
        fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    assert_eq!(
        open_store(&foreign_root).unwrap_err().kind(),
        HistoryErrorKind::UnsupportedSchema
    );

    let (_newer_directory, newer_root) = private_root("loxa-history-newer-");
    let (newer, _) = open_store(&newer_root).unwrap();
    newer.pragma_update(None, "user_version", 5).unwrap();
    newer.close().unwrap();
    assert_eq!(
        open_store(&newer_root).unwrap_err().kind(),
        HistoryErrorKind::UnsupportedSchema
    );

    let (_malformed_directory, malformed_root) = private_root("loxa-history-malformed-");
    let (malformed, _) = open_store(&malformed_root).unwrap();
    malformed
        .execute_batch("DROP INDEX conversations_recency")
        .unwrap();
    malformed.close().unwrap();
    assert_eq!(
        open_store(&malformed_root).unwrap_err().kind(),
        HistoryErrorKind::Corrupt
    );

    let (_extra_directory, extra_root) = private_root("loxa-history-extra-object-");
    let (extra, _) = open_store(&extra_root).unwrap();
    extra
        .execute_batch(
            "CREATE TRIGGER sqliteXafter_update AFTER UPDATE ON conversations BEGIN SELECT 1; END",
        )
        .unwrap();
    extra.close().unwrap();
    assert_eq!(
        open_store(&extra_root).unwrap_err().kind(),
        HistoryErrorKind::Corrupt
    );

    let (_unsafe_directory, unsafe_root) = private_root("loxa-history-unsafe-");
    let outside = unsafe_root.join("outside");
    fs::write(&outside, b"not a database").unwrap();
    symlink(&outside, unsafe_root.join("app.sqlite")).unwrap();
    assert_eq!(
        open_store(&unsafe_root).unwrap_err().kind(),
        HistoryErrorKind::UnsafePath
    );

    let (_mode_directory, mode_root) = private_root("loxa-history-mode-");
    let (mode_store, _) = open_store(&mode_root).unwrap();
    mode_store.close().unwrap();
    fs::set_permissions(
        mode_root.join("app.sqlite"),
        fs::Permissions::from_mode(0o700),
    )
    .unwrap();
    assert_eq!(
        open_store(&mode_root).unwrap_err().kind(),
        HistoryErrorKind::UnsafePath
    );
}

#[test]
fn conversation_crud_freezes_binding_and_uses_revision_fences() {
    let (_directory, root) = private_root("loxa-history-crud-");
    let models = root.join("models");
    fs::create_dir(&models).unwrap();
    install_local_manifest(&models, "demo");
    let unrelated = models.join("unrelated");
    fs::create_dir(&unrelated).unwrap();
    fs::write(unrelated.join("manifest.json"), b"not json").unwrap();
    let (mut connection, _) = open_store(&root).unwrap();

    let created = match conversations::execute(
        &mut connection,
        &models,
        RuntimeIdentity::LegacyCliB10121,
        WireCommand::CreateConversation {
            model_id: "demo".into(),
        },
    )
    .unwrap()
    {
        HistoryReply::Conversation(conversation) => conversation,
        _ => panic!("create returned the wrong history reply"),
    };
    assert_eq!(created.title, "New chat");
    assert_eq!(created.revision, "1");
    let frozen: (String, Vec<u8>, i64, String, i64) = connection
        .query_row(
            "SELECT primary_filename, primary_sha256, primary_size, system_instruction, max_output_tokens FROM conversations",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )
        .unwrap();
    assert_eq!(frozen.0, "model.gguf");
    assert_eq!(frozen.1.len(), 32);
    assert_eq!(frozen.2, 4);
    assert_eq!(frozen.3, "");
    assert_eq!(frozen.4, 512);

    let renamed = match conversations::execute(
        &mut connection,
        &models,
        RuntimeIdentity::LegacyCliB10121,
        WireCommand::RenameConversation {
            conversation_id: created.id.clone(),
            expected_revision: created.revision.clone(),
            title: "First chat".into(),
        },
    )
    .unwrap()
    {
        HistoryReply::Conversation(conversation) => conversation,
        _ => panic!("rename returned the wrong history reply"),
    };
    assert_eq!(renamed.revision, "2");
    let stale = conversations::execute(
        &mut connection,
        &models,
        RuntimeIdentity::LegacyCliB10121,
        WireCommand::RenameConversation {
            conversation_id: created.id.clone(),
            expected_revision: "1".into(),
            title: "Stale".into(),
        },
    )
    .unwrap_err();
    assert_eq!(stale.kind(), HistoryErrorKind::Conflict);

    let page = conversations::execute(
        &mut connection,
        &models,
        RuntimeIdentity::LegacyCliB10121,
        WireCommand::ListConversations {
            cursor: None,
            limit: 50,
        },
    )
    .unwrap();
    assert!(
        matches!(page, HistoryReply::ConversationPage(ref page) if page.conversations == [renamed.clone()] && page.next.is_none())
    );

    let deleted = conversations::execute(
        &mut connection,
        &models,
        RuntimeIdentity::LegacyCliB10121,
        WireCommand::DeleteConversation {
            conversation_id: renamed.id.clone(),
            expected_revision: renamed.revision,
        },
    )
    .unwrap();
    assert!(matches!(
        deleted,
        HistoryReply::ConversationDeleted {
            purge_complete: true,
            ..
        }
    ));
    let count: i64 = connection
        .query_row("SELECT count(*) FROM conversations", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 0);
    connection.close().unwrap();
}

#[test]
fn conversation_profiles_freeze_creation_defaults_reset_to_captured_globals_and_preserve_attempts()
{
    let (_directory, root) = private_root("loxa-history-profile-");
    let models = root.join("models");
    fs::create_dir(&models).unwrap();
    install_local_manifest(&models, "demo");
    let (mut connection, _) = open_store(&root).unwrap();
    let initial_default = GenerationSettings {
        system_instruction: "initial global".into(),
        max_output_tokens: 700,
        temperature: Some(SamplingValue::new(0.0).unwrap()),
        top_p: None,
    };
    let created = match conversations::execute_with_generation(
        &mut connection,
        &models,
        RuntimeIdentity::BundledB10344,
        WireCommand::CreateConversation {
            model_id: "demo".into(),
        },
        Some(initial_default.clone()),
    )
    .unwrap()
    {
        HistoryReply::Conversation(conversation) => conversation,
        _ => panic!("create returned the wrong history reply"),
    };
    let read = conversations::execute_profile(
        &mut connection,
        ServiceSettingsCommand::GetConversationProfile {
            conversation_id: created.id.clone(),
        },
        None,
    )
    .unwrap();
    assert_eq!(read.generation, initial_default);
    assert_eq!(read.conversation_revision, "1");
    assert_eq!(read.profile_revision, "1");

    let patched = conversations::execute_profile(
        &mut connection,
        ServiceSettingsCommand::PatchConversationProfile {
            conversation_id: created.id.clone(),
            expected_conversation_revision: "1".into(),
            expected_profile_revision: "1".into(),
            patch: GenerationSettingsPatch::Fields {
                system_instruction: Some("conversation override".into()),
                max_output_tokens: None,
                temperature: Some(OptionalSamplingValuePatch::Clear),
                top_p: Some(OptionalSamplingValuePatch::Set {
                    value: SamplingValue::new(0.7).unwrap(),
                }),
            },
        },
        None,
    )
    .unwrap();
    assert_eq!(patched.conversation_revision, "2");
    assert_eq!(patched.profile_revision, "2");
    assert_eq!(
        patched.generation.system_instruction,
        "conversation override"
    );
    assert_eq!(patched.generation.max_output_tokens, 700);
    assert_eq!(patched.generation.temperature, None);
    assert_eq!(patched.generation.top_p.unwrap().get(), 0.7);
    let stale = conversations::execute_profile(
        &mut connection,
        ServiceSettingsCommand::PatchConversationProfile {
            conversation_id: created.id.clone(),
            expected_conversation_revision: "1".into(),
            expected_profile_revision: "1".into(),
            patch: GenerationSettingsPatch::Fields {
                system_instruction: None,
                max_output_tokens: Some(701),
                temperature: None,
                top_p: None,
            },
        },
        None,
    )
    .unwrap_err();
    assert_eq!(stale.kind(), HistoryErrorKind::Conflict);

    let current_global = GenerationSettings {
        system_instruction: "new global".into(),
        max_output_tokens: 900,
        temperature: Some(SamplingValue::new(0.2).unwrap()),
        top_p: Some(SamplingValue::new(0.9).unwrap()),
    };
    let unchanged = conversations::execute_profile(
        &mut connection,
        ServiceSettingsCommand::GetConversationProfile {
            conversation_id: created.id.clone(),
        },
        None,
    )
    .unwrap();
    assert_eq!(unchanged.generation, patched.generation);
    let reset = conversations::execute_profile(
        &mut connection,
        ServiceSettingsCommand::PatchConversationProfile {
            conversation_id: created.id.clone(),
            expected_conversation_revision: "2".into(),
            expected_profile_revision: "2".into(),
            patch: GenerationSettingsPatch::Reset,
        },
        Some(current_global.clone()),
    )
    .unwrap();
    assert_eq!(reset.generation, current_global);
    assert_eq!(reset.conversation_revision, "3");
    assert_eq!(reset.profile_revision, "3");
    connection.close().unwrap();
    let (mut connection, _) = open_store(&root).unwrap();
    let reopened = conversations::execute_profile(
        &mut connection,
        ServiceSettingsCommand::GetConversationProfile {
            conversation_id: created.id.clone(),
        },
        None,
    )
    .unwrap();
    assert_eq!(reopened.generation, current_global);

    let prepared = PreparedAdmission::new(
        identity::decode_id(&created.id).unwrap(),
        [91; 16],
        [92; 32],
        3,
        3,
        "boot-1".into(),
        91,
        Arc::new(generic_service_fingerprint()),
        RuntimeIdentity::BundledB10344,
        4096,
        current_global.system_instruction.clone(),
        i64::from(current_global.max_output_tokens),
        effective_sampling(),
        PromptBasis {
            references: Vec::new(),
        },
        AdmissionKind::Send {
            user_text: "question".into(),
            draft: None,
        },
    )
    .unwrap();
    admission::admit_send(&mut connection, &prepared).unwrap();
    let changed_after_admission = conversations::execute_profile(
        &mut connection,
        ServiceSettingsCommand::PatchConversationProfile {
            conversation_id: created.id,
            expected_conversation_revision: "4".into(),
            expected_profile_revision: "3".into(),
            patch: GenerationSettingsPatch::Fields {
                system_instruction: Some("later profile".into()),
                max_output_tokens: Some(1000),
                temperature: None,
                top_p: None,
            },
        },
        None,
    )
    .unwrap();
    assert_eq!(changed_after_admission.profile_revision, "4");
    let frozen_attempt: (String, i64, i64) = connection
        .query_row(
            "SELECT system_instruction, max_output_tokens, admitted_profile_revision FROM attempts",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(frozen_attempt, ("new global".into(), 900, 3));
    let frozen_sampling: (f64, f64) = connection
        .query_row(
            "SELECT temperature, top_p FROM attempt_sampling",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(frozen_sampling, (0.8, 0.95));
    let desired_sampling: (f64, f64) = connection
        .query_row(
            "SELECT temperature, top_p FROM conversation_sampling",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(desired_sampling, (0.2, 0.9));
}

#[test]
fn bundle_creation_freezes_primary_draft_provenance_and_qualified_profile() {
    let (_directory, root) = private_root("loxa-history-bundle-");
    let models = root.join("models");
    fs::create_dir(&models).unwrap();
    install_bundle_manifest(&models);
    let (mut connection, _) = open_store(&root).unwrap();

    conversations::execute(
        &mut connection,
        &models,
        RuntimeIdentity::BundledB10344,
        WireCommand::CreateConversation {
            model_id: "gemma4".into(),
        },
    )
    .unwrap();
    let binding = connection
        .query_row(
            "SELECT binding_profile, qualified_profile, qualified_engine,
                        qualified_engine_build, primary_sha256, draft_sha256,
                        primary_source_kind, draft_source_repo
                 FROM conversations",
            [],
            |row| {
                Ok(StoredBundleBinding {
                    profile_kind: row.get(0)?,
                    profile: row.get(1)?,
                    engine: row.get(2)?,
                    engine_build: row.get(3)?,
                    primary_sha256: row.get(4)?,
                    draft_sha256: row.get(5)?,
                    primary_source_kind: row.get(6)?,
                    draft_source_repo: row.get(7)?,
                })
            },
        )
        .unwrap();
    assert_eq!(binding.profile_kind, 1);
    assert_eq!(binding.profile, GEMMA4_MTP_PROFILE);
    assert_eq!(binding.engine, "llama.cpp");
    assert_eq!(binding.engine_build, GEMMA4_LLAMA_BUILD);
    assert_eq!(binding.primary_sha256.len(), 32);
    assert_eq!(binding.draft_sha256.len(), 32);
    assert_eq!(binding.primary_source_kind, 1);
    assert_eq!(binding.draft_source_repo, None);
    assert!(connection
        .execute("UPDATE conversations SET draft_size = NULL", [])
        .is_err());
    assert!(connection
        .execute("UPDATE conversations SET draft_source_kind = NULL", [])
        .is_err());
    connection.close().unwrap();
}

#[test]
fn draft_consumption_is_atomic_and_preserves_a_newer_snapshot() {
    let (_directory, root) = private_root("loxa-history-draft-admission-");
    let models = root.join("models");
    fs::create_dir(&models).unwrap();
    install_local_manifest(&models, "demo");
    let (mut connection, _) = open_store(&root).unwrap();
    let conversation = create_local_conversation(&mut connection, &models);
    let conversation_id = identity::decode_id(&conversation.id).unwrap();
    let client = [7_u8; 16];
    let client_id = identity::encode_id(client);

    let draft = match drafts::execute(
        &mut connection,
        DraftCommand::CreateScope {
            desktop_client_id: client_id.clone(),
            conversation_id: None,
        },
    )
    .unwrap()
    {
        DraftReply::Snapshot(snapshot) => snapshot,
        _ => panic!("create scope returned the wrong reply"),
    };
    let draft_id = identity::decode_id(&draft.id).unwrap();
    let saved = match drafts::execute(
        &mut connection,
        DraftCommand::SaveSnapshot {
            draft_id: draft.id.clone(),
            desktop_client_id: client_id.clone(),
            revision: "5".into(),
            text: "send me".into(),
        },
    )
    .unwrap()
    {
        DraftReply::Snapshot(snapshot) => snapshot,
        _ => panic!("save returned the wrong reply"),
    };
    assert_eq!(saved.revision, "5");

    let prepared = prepared_admission(
        conversation_id,
        1,
        1,
        11,
        41,
        AdmissionKind::Send {
            user_text: "send me".into(),
            draft: Some(DraftSubmission {
                id: draft_id,
                desktop_client_id: client,
                revision: 5,
            }),
        },
    );
    let committed = admission::admit_send(&mut connection, &prepared).unwrap();
    assert_eq!(committed.pre_conversation_revision, 1);
    assert_eq!(committed.post_conversation_revision, 2);
    let consumed = match drafts::execute(
        &mut connection,
        DraftCommand::ReadScope {
            draft_id: draft.id.clone(),
            desktop_client_id: client_id.clone(),
        },
    )
    .unwrap()
    {
        DraftReply::Snapshot(snapshot) => snapshot,
        _ => panic!("read returned the wrong reply"),
    };
    assert_eq!(
        consumed.conversation_id.as_deref(),
        Some(conversation.id.as_str())
    );
    assert_eq!(consumed.revision, "5");
    assert_eq!(consumed.consumed_revision, "5");
    assert!(consumed.text.is_empty());

    let replay = admission::admit_send(&mut connection, &prepared).unwrap();
    assert_eq!(replay, committed);
    let changed = prepared_admission(
        conversation_id,
        1,
        1,
        12,
        41,
        AdmissionKind::Send {
            user_text: "send me".into(),
            draft: Some(DraftSubmission {
                id: draft_id,
                desktop_client_id: client,
                revision: 5,
            }),
        },
    );
    assert_eq!(
        admission::admit_send(&mut connection, &changed)
            .unwrap_err()
            .kind(),
        HistoryErrorKind::Conflict
    );

    admission::stop_before_execution(&connection, &committed).unwrap();
    drafts::execute(
        &mut connection,
        DraftCommand::SaveSnapshot {
            draft_id: draft.id.clone(),
            desktop_client_id: client_id.clone(),
            revision: "6".into(),
            text: "newer edit".into(),
        },
    )
    .unwrap();
    for (revision, text) in [("4", "old"), ("5", "send me"), ("6", "changed")] {
        assert_eq!(
            drafts::execute(
                &mut connection,
                DraftCommand::SaveSnapshot {
                    draft_id: draft.id.clone(),
                    desktop_client_id: client_id.clone(),
                    revision: revision.into(),
                    text: text.into(),
                },
            )
            .unwrap_err()
            .kind(),
            HistoryErrorKind::Conflict
        );
    }
    let stale_new_submission = prepared_admission(
        conversation_id,
        2,
        2,
        22,
        42,
        AdmissionKind::Send {
            user_text: "send me".into(),
            draft: Some(DraftSubmission {
                id: draft_id,
                desktop_client_id: client,
                revision: 5,
            }),
        },
    );
    assert_eq!(
        admission::admit_send(&mut connection, &stale_new_submission)
            .unwrap_err()
            .kind(),
        HistoryErrorKind::Conflict
    );
    let current = match drafts::execute(
        &mut connection,
        DraftCommand::ReadScope {
            draft_id: draft.id,
            desktop_client_id: client_id,
        },
    )
    .unwrap()
    {
        DraftReply::Snapshot(snapshot) => snapshot,
        _ => panic!("read returned the wrong reply"),
    };
    assert_eq!(current.revision, "6");
    assert_eq!(current.consumed_revision, "5");
    assert_eq!(current.text, "newer edit");
    let counts: (i64, i64) = connection
        .query_row(
            "SELECT (SELECT COUNT(*) FROM turns), (SELECT COUNT(*) FROM attempts)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(counts, (1, 1));
    connection.close().unwrap();
}

#[test]
fn late_admission_sql_failure_rolls_back_send_and_retry_atomically() {
    let (_directory, root) = private_root("loxa-history-admission-rollback-");
    let models = root.join("models");
    fs::create_dir(&models).unwrap();
    install_local_manifest(&models, "demo");
    let (mut connection, _) = open_store(&root).unwrap();
    let conversation = create_local_conversation(&mut connection, &models);
    let conversation_id = identity::decode_id(&conversation.id).unwrap();
    let conversation_state = |connection: &Connection| -> (String, String, i64, i64, i64) {
        connection
            .query_row(
                "SELECT title, system_instruction, max_output_tokens, revision, profile_revision
                 FROM conversations WHERE id = ?1",
                [conversation_id.as_slice()],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .unwrap()
    };
    let client = [8_u8; 16];
    let client_id = identity::encode_id(client);
    let draft = match drafts::execute(
        &mut connection,
        DraftCommand::CreateScope {
            desktop_client_id: client_id.clone(),
            conversation_id: None,
        },
    )
    .unwrap()
    {
        DraftReply::Snapshot(snapshot) => snapshot,
        _ => panic!("create scope returned the wrong reply"),
    };
    let draft_id = identity::decode_id(&draft.id).unwrap();
    let saved = match drafts::execute(
        &mut connection,
        DraftCommand::SaveSnapshot {
            draft_id: draft.id.clone(),
            desktop_client_id: client_id.clone(),
            revision: "1".into(),
            text: "rollback me".into(),
        },
    )
    .unwrap()
    {
        DraftReply::Snapshot(snapshot) => snapshot,
        _ => panic!("save returned the wrong reply"),
    };
    let prepared = prepared_admission(
        conversation_id,
        1,
        21,
        21,
        51,
        AdmissionKind::Send {
            user_text: "rollback me".into(),
            draft: Some(DraftSubmission {
                id: draft_id,
                desktop_client_id: client,
                revision: 1,
            }),
        },
    );
    let original_conversation = conversation_state(&connection);
    connection
        .execute_batch(
            "CREATE TEMP TRIGGER fail_late_admission
             BEFORE UPDATE OF revision ON conversations
             BEGIN SELECT RAISE(ABORT, 'injected late admission failure'); END;",
        )
        .unwrap();

    assert_eq!(
        admission::admit_send(&mut connection, &prepared)
            .unwrap_err()
            .kind(),
        HistoryErrorKind::Io
    );
    let current_draft = match drafts::execute(
        &mut connection,
        DraftCommand::ReadScope {
            draft_id: draft.id.clone(),
            desktop_client_id: client_id,
        },
    )
    .unwrap()
    {
        DraftReply::Snapshot(snapshot) => snapshot,
        _ => panic!("read returned the wrong reply"),
    };
    assert_eq!(current_draft, saved);
    let unchanged_conversation = conversation_state(&connection);
    assert_eq!(unchanged_conversation, original_conversation);
    let counts: (i64, i64, i64) = connection
        .query_row(
            "SELECT (SELECT COUNT(*) FROM turns), (SELECT COUNT(*) FROM attempts),
                    (SELECT COUNT(*) FROM attempt_sampling)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(counts, (0, 0, 0));

    connection
        .execute_batch("DROP TRIGGER fail_late_admission")
        .unwrap();
    let committed = admission::admit_send(&mut connection, &prepared).unwrap();
    admission::stop_before_execution(&connection, &committed).unwrap();
    let retry = prepared_admission(
        conversation_id,
        2,
        22,
        22,
        52,
        AdmissionKind::Retry {
            prior_attempt_id: committed.attempt_id,
        },
    );
    let before_retry = conversation_state(&connection);
    let selected_before: Vec<u8> = connection
        .query_row("SELECT selected_attempt_id FROM turns", [], |row| {
            row.get(0)
        })
        .unwrap();
    connection
        .execute_batch(
            "CREATE TEMP TRIGGER fail_late_admission
             BEFORE UPDATE OF revision ON conversations
             BEGIN SELECT RAISE(ABORT, 'injected late admission failure'); END;",
        )
        .unwrap();

    assert_eq!(
        admission::admit_retry(&mut connection, &retry)
            .unwrap_err()
            .kind(),
        HistoryErrorKind::Io
    );
    let after_retry = conversation_state(&connection);
    assert_eq!(after_retry, before_retry);
    let selected_after: Vec<u8> = connection
        .query_row("SELECT selected_attempt_id FROM turns", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(selected_after, selected_before);
    let attempts: (i64, i64) = connection
        .query_row(
            "SELECT (SELECT COUNT(*) FROM attempts),
                    (SELECT COUNT(*) FROM attempt_sampling)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(attempts, (1, 1));

    connection
        .execute_batch("DROP TRIGGER fail_late_admission")
        .unwrap();
    let retried = admission::admit_retry(&mut connection, &retry).unwrap();
    assert_ne!(retried.attempt_id, committed.attempt_id);
    let selected: Vec<u8> = connection
        .query_row("SELECT selected_attempt_id FROM turns", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(selected, retried.attempt_id);
    let attempts: i64 = connection
        .query_row("SELECT COUNT(*) FROM attempts", [], |row| row.get(0))
        .unwrap();
    assert_eq!(attempts, 2);
    connection.close().unwrap();
}

#[test]
fn retry_targets_only_the_latest_selected_terminal_attempt() {
    let (_directory, root) = private_root("loxa-history-retry-");
    let models = root.join("models");
    fs::create_dir(&models).unwrap();
    install_local_manifest(&models, "demo");
    let (mut connection, _) = open_store(&root).unwrap();
    let conversation = create_local_conversation(&mut connection, &models);
    let conversation_id = identity::decode_id(&conversation.id).unwrap();

    let first = admission::admit_send(
        &mut connection,
        &prepared_admission(
            conversation_id,
            1,
            1,
            1,
            41,
            AdmissionKind::Send {
                user_text: "h😀".into(),
                draft: None,
            },
        ),
    )
    .unwrap();
    admission::stop_before_execution(&connection, &first).unwrap();
    let invalid_basis = prepared_admission_with_basis(
        conversation_id,
        2,
        9,
        9,
        42,
        AdmissionKind::Retry {
            prior_attempt_id: first.attempt_id,
        },
        PromptBasis {
            references: vec![PromptReference {
                turn_id: first.turn_id,
                attempt_id: Some(first.attempt_id),
                prefix_end: 0,
            }],
        },
    );
    assert_eq!(
        admission::admit_retry(&mut connection, &invalid_basis)
            .unwrap_err()
            .kind(),
        HistoryErrorKind::Conflict
    );
    for prefix_end in [2, 6] {
        let invalid_user_basis = prepared_admission_with_basis(
            conversation_id,
            2,
            10,
            10,
            42,
            AdmissionKind::Retry {
                prior_attempt_id: first.attempt_id,
            },
            PromptBasis {
                references: vec![PromptReference {
                    turn_id: first.turn_id,
                    attempt_id: None,
                    prefix_end,
                }],
            },
        );
        assert_eq!(
            admission::admit_retry(&mut connection, &invalid_user_basis)
                .unwrap_err()
                .kind(),
            HistoryErrorKind::Conflict
        );
    }
    let out_of_range_basis = prepared_admission_with_basis(
        conversation_id,
        2,
        11,
        11,
        42,
        AdmissionKind::Retry {
            prior_attempt_id: first.attempt_id,
        },
        PromptBasis {
            references: vec![PromptReference {
                turn_id: first.turn_id,
                attempt_id: None,
                prefix_end: u64::MAX,
            }],
        },
    );
    assert_eq!(
        admission::admit_retry(&mut connection, &out_of_range_basis)
            .unwrap_err()
            .kind(),
        HistoryErrorKind::InvalidInput
    );
    let second_prepared = prepared_admission_with_basis(
        conversation_id,
        2,
        2,
        2,
        42,
        AdmissionKind::Retry {
            prior_attempt_id: first.attempt_id,
        },
        PromptBasis {
            references: vec![PromptReference {
                turn_id: first.turn_id,
                attempt_id: None,
                prefix_end: 5,
            }],
        },
    );
    let second = admission::admit_retry(&mut connection, &second_prepared).unwrap();
    assert_eq!(second.turn_id, first.turn_id);
    assert_ne!(second.attempt_id, first.attempt_id);
    assert_eq!(second.post_conversation_revision, 3);
    assert_eq!(
        admission::admit_retry(&mut connection, &second_prepared).unwrap(),
        second
    );

    let stale = prepared_admission(
        conversation_id,
        3,
        3,
        3,
        43,
        AdmissionKind::Retry {
            prior_attempt_id: first.attempt_id,
        },
    );
    assert_eq!(
        admission::admit_retry(&mut connection, &stale)
            .unwrap_err()
            .kind(),
        HistoryErrorKind::Conflict
    );
    admission::stop_before_execution(&connection, &second).unwrap();
    let third = admission::admit_retry(
        &mut connection,
        &prepared_admission(
            conversation_id,
            3,
            4,
            4,
            44,
            AdmissionKind::Retry {
                prior_attempt_id: second.attempt_id,
            },
        ),
    )
    .unwrap();
    assert_eq!(third.turn_id, first.turn_id);
    let selected: Vec<u8> = connection
        .query_row("SELECT selected_attempt_id FROM turns", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(selected, third.attempt_id);
    let attempts: i64 = connection
        .query_row("SELECT COUNT(*) FROM attempts", [], |row| row.get(0))
        .unwrap();
    assert_eq!(attempts, 3);
    assert!(connection
        .execute(
            "UPDATE attempts SET execution_outcome = 2, save_outcome = 1,
             generated_end = 0, saved_end = 0, terminal_saved_end = NULL
             WHERE id = ?1",
            [third.attempt_id.as_slice()],
        )
        .is_err());
    connection.close().unwrap();
}

#[test]
fn retry_prompt_keeps_the_stored_user_and_excludes_only_the_replaced_assistant() {
    let (_directory, root) = private_root("loxa-history-retry-prompt-");
    let models = root.join("models");
    fs::create_dir(&models).unwrap();
    install_local_manifest(&models, "demo");
    let (mut connection, _) = open_store(&root).unwrap();
    let conversation = create_local_conversation(&mut connection, &models);
    let conversation_id = identity::decode_id(&conversation.id).unwrap();

    let first = admission::admit_send(
        &mut connection,
        &prepared_admission(
            conversation_id,
            1,
            61,
            61,
            61,
            AdmissionKind::Send {
                user_text: "first user".into(),
                draft: None,
            },
        ),
    )
    .unwrap();
    content::finalize(
        &mut connection,
        &FinalizationInput {
            suffix: suffix_input(&first, 61, 0, "first assistant"),
            execution_outcome: ExecutionOutcome::Completed,
            generated_end: 15,
            failure_code: None,
            statistics: None,
        },
    )
    .unwrap();
    let second = admission::admit_send(
        &mut connection,
        &prepared_admission(
            conversation_id,
            2,
            62,
            62,
            62,
            AdmissionKind::Send {
                user_text: "retry this user".into(),
                draft: None,
            },
        ),
    )
    .unwrap();
    content::finalize(
        &mut connection,
        &FinalizationInput {
            suffix: suffix_input(&second, 62, 0, "replace this assistant"),
            execution_outcome: ExecutionOutcome::Completed,
            generated_end: 22,
            failure_code: None,
            statistics: None,
        },
    )
    .unwrap();

    let prepared = prompt::prepare(
        &connection,
        conversation_id,
        3,
        1,
        PromptRequest::Retry {
            prior_attempt_id: second.attempt_id,
        },
    )
    .unwrap();
    assert_eq!(
        prepared.messages,
        [
            PromptMessage {
                role: PromptRole::User,
                content: "first user".into(),
            },
            PromptMessage {
                role: PromptRole::Assistant,
                content: "first assistant".into(),
            },
            PromptMessage {
                role: PromptRole::User,
                content: "retry this user".into(),
            },
        ]
    );
    assert_eq!(
        prepared.basis.references,
        [
            PromptReference {
                turn_id: first.turn_id,
                attempt_id: None,
                prefix_end: 10,
            },
            PromptReference {
                turn_id: first.turn_id,
                attempt_id: Some(first.attempt_id),
                prefix_end: 15,
            },
            PromptReference {
                turn_id: second.turn_id,
                attempt_id: None,
                prefix_end: 15,
            },
        ]
    );
    assert_eq!(
        prompt::prepare(
            &connection,
            conversation_id,
            3,
            2,
            PromptRequest::Retry {
                prior_attempt_id: second.attempt_id,
            },
        )
        .unwrap_err()
        .kind(),
        HistoryErrorKind::Conflict
    );
    connection
        .execute(
            "UPDATE conversations SET deleted = 1 WHERE id = ?1",
            [conversation_id.as_slice()],
        )
        .unwrap();
    assert_eq!(
        prompt::prepare(
            &connection,
            conversation_id,
            3,
            1,
            PromptRequest::Retry {
                prior_attempt_id: second.attempt_id,
            },
        )
        .unwrap_err()
        .kind(),
        HistoryErrorKind::NotFound
    );
    connection.close().unwrap();
}

#[test]
fn suffixes_finalize_exactly_and_ranges_keep_a_captured_prefix() {
    let (_directory, root) = private_root("loxa-history-content-");
    let models = root.join("models");
    fs::create_dir(&models).unwrap();
    install_local_manifest(&models, "demo");
    let (mut connection, _) = open_store(&root).unwrap();
    let (_, committed) = admitted_attempt(&mut connection, &models, 21, 21);

    let first = suffix_input(&committed, 21, 0, "Aé");
    assert_eq!(
        content::append_suffix(&mut connection, &first).unwrap(),
        SuffixCommit {
            start: 0,
            end: 3,
            current_saved_end: 3,
        }
    );
    let captured = content::capture_attempt_prefix(&connection, committed.attempt_id).unwrap();
    assert_eq!(captured, 3);
    let second = suffix_input(&committed, 21, 3, "🙂B");
    content::append_suffix(&mut connection, &second).unwrap();
    let replay = content::append_suffix(&mut connection, &first).unwrap();
    assert_eq!(replay.end, 3);
    assert_eq!(replay.current_saved_end, 8);

    let mut changed = first.clone();
    changed.content = "Ace".into();
    assert_eq!(
        content::append_suffix(&mut connection, &changed)
            .unwrap_err()
            .kind(),
        HistoryErrorKind::Conflict
    );
    assert_eq!(
        content::append_suffix(&mut connection, &suffix_input(&committed, 21, 4, "gap"))
            .unwrap_err()
            .kind(),
        HistoryErrorKind::Conflict
    );
    assert_eq!(
        content::read_attempt_range(&connection, committed.attempt_id, 0, captured).unwrap(),
        ContentRange {
            start: 0,
            end: 3,
            prefix_end: 3,
            content: "Aé".into(),
        }
    );
    assert_eq!(
        content::read_attempt_range(&connection, committed.attempt_id, 2, 8)
            .unwrap_err()
            .kind(),
        HistoryErrorKind::InvalidInput
    );

    let finalization = FinalizationInput {
        suffix: suffix_input(&committed, 21, 8, "終"),
        execution_outcome: ExecutionOutcome::Completed,
        generated_end: 11,
        failure_code: None,
        statistics: Some(AttemptStatistics {
            qualified_input_tokens: Some(7),
            qualified_output_tokens: Some(3),
            service_first_output_latency_ms: Some(4),
            qualified_engine_decode_tokens_per_second: Some(50.0),
            service_total_duration_ms: 10,
            stop_reason: loxa_ipc::AttemptStopReason::Completed,
        }),
    };
    assert_eq!(
        content::finalize(&mut connection, &finalization)
            .unwrap()
            .end,
        11
    );
    let mut changed_statistics = finalization.clone();
    changed_statistics
        .statistics
        .as_mut()
        .unwrap()
        .qualified_output_tokens = Some(4);
    assert_eq!(
        content::finalize(&mut connection, &changed_statistics)
            .unwrap_err()
            .kind(),
        HistoryErrorKind::Conflict
    );
    assert_eq!(
        content::finalize(&mut connection, &finalization)
            .unwrap()
            .end,
        11
    );
    let empty_replay = FinalizationInput {
        suffix: suffix_input(&committed, 21, 11, ""),
        execution_outcome: ExecutionOutcome::Completed,
        generated_end: 11,
        failure_code: None,
        statistics: None,
    };
    assert_eq!(
        content::finalize(&mut connection, &empty_replay)
            .unwrap_err()
            .kind(),
        HistoryErrorKind::Conflict
    );
    let changed_outcome = FinalizationInput {
        suffix: finalization.suffix.clone(),
        execution_outcome: ExecutionOutcome::Failed,
        generated_end: 11,
        failure_code: Some("stopped".into()),
        statistics: None,
    };
    assert_eq!(
        content::finalize(&mut connection, &changed_outcome)
            .unwrap_err()
            .kind(),
        HistoryErrorKind::Conflict
    );
    connection.close().unwrap();
}

#[test]
fn terminal_statistics_and_outcome_roll_back_together() {
    let (_directory, root) = private_root("loxa-history-statistics-atomic-");
    let models = root.join("models");
    fs::create_dir(&models).unwrap();
    install_local_manifest(&models, "demo");
    let (mut connection, _) = open_store(&root).unwrap();
    let (_, committed) = admitted_attempt(&mut connection, &models, 22, 22);
    connection
        .execute_batch(
            "CREATE TRIGGER fail_terminal_update BEFORE UPDATE OF execution_outcome ON attempts
             WHEN NEW.execution_outcome != 0 BEGIN SELECT RAISE(ABORT, 'injected'); END",
        )
        .unwrap();
    let finalization = FinalizationInput {
        suffix: suffix_input(&committed, 22, 0, "answer"),
        execution_outcome: ExecutionOutcome::Completed,
        generated_end: 6,
        failure_code: None,
        statistics: Some(AttemptStatistics {
            qualified_input_tokens: Some(5),
            qualified_output_tokens: None,
            service_first_output_latency_ms: Some(1),
            qualified_engine_decode_tokens_per_second: None,
            service_total_duration_ms: 2,
            stop_reason: loxa_ipc::AttemptStopReason::Completed,
        }),
    };
    assert!(content::finalize(&mut connection, &finalization).is_err());
    let rolled_back: (i64, i64, i64, i64, i64) = connection
        .query_row(
            "SELECT execution_outcome, save_outcome,
                    (SELECT COUNT(*) FROM attempt_chunks),
                    (SELECT COUNT(*) FROM attempt_finalizations),
                    (SELECT COUNT(*) FROM attempt_statistics)
             FROM attempts WHERE id = ?1",
            [committed.attempt_id.as_slice()],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(rolled_back, (0, 0, 0, 0, 0));
    connection
        .execute_batch("DROP TRIGGER fail_terminal_update")
        .unwrap();
    assert_eq!(
        content::finalize(&mut connection, &finalization)
            .unwrap()
            .end,
        6
    );
    connection.close().unwrap();
}

#[test]
fn turn_metadata_and_content_reads_are_bounded_to_a_captured_prefix() {
    let (_directory, root) = private_root("loxa-history-read-");
    let models = root.join("models");
    fs::create_dir(&models).unwrap();
    install_local_manifest(&models, "demo");
    let (mut connection, _) = open_store(&root).unwrap();
    let (conversation, committed) = admitted_attempt(&mut connection, &models, 34, 34);
    content::append_suffix(&mut connection, &suffix_input(&committed, 34, 0, "Aé")).unwrap();

    let page = reads::list_turns(&connection, &conversation.id, None, 50).unwrap();
    assert_eq!(page.turns.len(), 1);
    assert!(page.next.is_none());
    let turn = &page.turns[0];
    assert_eq!(turn.ordinal, "1");
    assert_eq!(turn.user_text_end, "8");
    let attempt = turn.selected_attempt.as_ref().unwrap();
    assert_eq!(attempt.id, identity::encode_id(committed.attempt_id));
    assert_eq!(attempt.saved_end, "3");
    assert_eq!(attempt.execution, loxa_ipc::AttemptExecution::Pending);
    assert_eq!(attempt.save, loxa_ipc::AttemptSave::Open);
    assert_eq!(attempt.effective_sampling, Some(effective_sampling()));
    assert!(connection
        .execute(
            "UPDATE attempt_sampling SET temperature = -1.0 WHERE attempt_id = ?1",
            [committed.attempt_id.as_slice()],
        )
        .is_err());
    assert!(connection
        .execute(
            "UPDATE attempt_sampling SET top_p = NULL WHERE attempt_id = ?1",
            [committed.attempt_id.as_slice()],
        )
        .is_err());

    content::append_suffix(&mut connection, &suffix_input(&committed, 34, 3, "later")).unwrap();
    let assistant = reads::read_content_range(
        &connection,
        loxa_ipc::ContentSource::Assistant {
            attempt_id: attempt.id.clone(),
        },
        "0",
        "3",
    )
    .unwrap();
    assert_eq!(assistant.content, "Aé");
    assert_eq!(assistant.prefix_end, "3");
    let user = reads::read_content_range(
        &connection,
        loxa_ipc::ContentSource::User {
            turn_id: turn.id.clone(),
        },
        "0",
        &turn.user_text_end,
    )
    .unwrap();
    assert_eq!(user.content, "question");
    assert_eq!(user.end, "8");

    connection
        .execute_batch("PRAGMA ignore_check_constraints = ON")
        .unwrap();
    connection
        .execute(
            "UPDATE attempt_sampling SET top_p = 2.0 WHERE attempt_id = ?1",
            [committed.attempt_id.as_slice()],
        )
        .unwrap();
    assert_eq!(
        reads::list_turns(&connection, &conversation.id, None, 50)
            .unwrap_err()
            .kind(),
        HistoryErrorKind::Corrupt
    );
    connection
        .execute(
            "UPDATE attempt_sampling SET top_p = 0.95 WHERE attempt_id = ?1",
            [committed.attempt_id.as_slice()],
        )
        .unwrap();

    connection
        .execute(
            "UPDATE conversations SET deleted = 1 WHERE id = ?1",
            [identity::decode_id(&conversation.id).unwrap().as_slice()],
        )
        .unwrap();
    assert_eq!(
        reads::list_turns(&connection, &conversation.id, None, 1)
            .unwrap_err()
            .kind(),
        HistoryErrorKind::NotFound
    );
    connection.close().unwrap();
}

#[test]
fn turn_reads_reject_saved_nulls_and_cross_turn_selected_attempts() {
    let (_directory, root) = private_root("loxa-history-corrupt-turn-read-");
    let models = root.join("models");
    fs::create_dir(&models).unwrap();
    install_local_manifest(&models, "demo");
    let (mut connection, _) = open_store(&root).unwrap();
    let (first_conversation, first) = admitted_attempt(&mut connection, &models, 36, 36);
    content::finalize(
        &mut connection,
        &FinalizationInput {
            suffix: suffix_input(&first, 36, 0, ""),
            execution_outcome: ExecutionOutcome::Completed,
            generated_end: 0,
            failure_code: None,
            statistics: None,
        },
    )
    .unwrap();
    connection
        .execute_batch("PRAGMA ignore_check_constraints = ON")
        .unwrap();
    connection
        .execute(
            "UPDATE attempts SET terminal_saved_end = NULL WHERE id = ?1",
            [first.attempt_id.as_slice()],
        )
        .unwrap();
    assert_eq!(
        reads::list_turns(&connection, &first_conversation.id, None, 1)
            .unwrap_err()
            .kind(),
        HistoryErrorKind::Corrupt
    );
    connection
        .execute(
            "UPDATE attempts SET terminal_saved_end = 0 WHERE id = ?1",
            [first.attempt_id.as_slice()],
        )
        .unwrap();
    connection
        .execute_batch("PRAGMA ignore_check_constraints = OFF")
        .unwrap();

    let (_second_conversation, second) = admitted_attempt(&mut connection, &models, 37, 37);
    connection
        .execute(
            "UPDATE turns SET selected_attempt_id = ?1 WHERE id = ?2",
            rusqlite::params![second.attempt_id.as_slice(), first.turn_id.as_slice()],
        )
        .unwrap();
    assert_eq!(
        reads::list_turns(&connection, &first_conversation.id, None, 1)
            .unwrap_err()
            .kind(),
        HistoryErrorKind::Corrupt
    );
    connection.close().unwrap();
}

#[test]
fn sqlite_full_and_late_update_failure_roll_back_suffix_transaction() {
    let (_directory, root) = private_root("loxa-history-content-disk-full-");
    let models = root.join("models");
    fs::create_dir(&models).unwrap();
    install_local_manifest(&models, "demo");
    let (mut connection, _) = open_store(&root).unwrap();
    let (_, committed) = admitted_attempt(&mut connection, &models, 33, 33);

    let previous_max_pages: i64 = connection
        .query_row("PRAGMA max_page_count", [], |row| row.get(0))
        .unwrap();
    let page_count: i64 = connection
        .query_row("PRAGMA page_count", [], |row| row.get(0))
        .unwrap();
    let free_pages: i64 = connection
        .query_row("PRAGMA freelist_count", [], |row| row.get(0))
        .unwrap();
    assert_eq!(free_pages, 0);
    let capped_pages: i64 = connection
        .query_row(
            &format!("PRAGMA max_page_count = {page_count}"),
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(capped_pages, page_count);
    let suffix = suffix_input(&committed, 33, 0, &"x".repeat(content::MAX_SUFFIX_BYTES));
    assert_eq!(
        content::append_suffix(&mut connection, &suffix)
            .unwrap_err()
            .kind(),
        HistoryErrorKind::DiskFull
    );
    let saved_end: i64 = connection
        .query_row(
            "SELECT saved_end FROM attempts WHERE id = ?1",
            [committed.attempt_id.as_slice()],
            |row| row.get(0),
        )
        .unwrap();
    let chunk_count: i64 = connection
        .query_row(
            "SELECT count(*) FROM attempt_chunks WHERE attempt_id = ?1",
            [committed.attempt_id.as_slice()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(saved_end, 0);
    assert_eq!(chunk_count, 0);

    let restored_pages: i64 = connection
        .query_row(
            &format!("PRAGMA max_page_count = {previous_max_pages}"),
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(restored_pages, previous_max_pages);
    assert_eq!(
        content::append_suffix(&mut connection, &suffix)
            .unwrap()
            .end,
        content::MAX_SUFFIX_BYTES as u64
    );

    connection
        .execute_batch(
            "CREATE TEMP TRIGGER fail_suffix_parent_update
             BEFORE UPDATE OF saved_end ON attempts
             BEGIN SELECT RAISE(ABORT, 'injected late suffix failure'); END;",
        )
        .unwrap();
    let tail = suffix_input(&committed, 33, content::MAX_SUFFIX_BYTES as u64, "tail");
    assert_eq!(
        content::append_suffix(&mut connection, &tail)
            .unwrap_err()
            .kind(),
        HistoryErrorKind::Io
    );
    let state: (i64, i64) = connection
        .query_row(
            "SELECT saved_end,
                    (SELECT count(*) FROM attempt_chunks WHERE attempt_id = attempts.id)
             FROM attempts WHERE id = ?1",
            [committed.attempt_id.as_slice()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(state, (content::MAX_SUFFIX_BYTES as i64, 1));

    connection
        .execute_batch("DROP TRIGGER fail_suffix_parent_update")
        .unwrap();
    assert_eq!(
        content::append_suffix(&mut connection, &tail).unwrap().end,
        content::MAX_SUFFIX_BYTES as u64 + 4
    );
    connection.close().unwrap();
}

#[test]
fn deep_content_range_seeks_from_the_preceding_chunk() {
    let (_directory, root) = private_root("loxa-history-content-seek-");
    let models = root.join("models");
    fs::create_dir(&models).unwrap();
    install_local_manifest(&models, "demo");
    let (mut connection, _) = open_store(&root).unwrap();
    let (_, committed) = admitted_attempt(&mut connection, &models, 22, 22);
    for offset in 0..40 {
        content::append_suffix(&mut connection, &suffix_input(&committed, 22, offset, "x"))
            .unwrap();
    }
    let range = content::read_attempt_range(&connection, committed.attempt_id, 37, 40).unwrap();
    assert_eq!(range.content, "xxx");
    assert_eq!(range.end, 40);

    let plan = connection
        .prepare(
            "EXPLAIN QUERY PLAN SELECT start_offset, end_offset, content FROM attempt_chunks
             WHERE attempt_id = ?1
               AND start_offset >= COALESCE((SELECT MAX(start_offset) FROM attempt_chunks
                   WHERE attempt_id = ?1 AND start_offset <= ?2), ?2)
               AND start_offset < ?3 ORDER BY start_offset LIMIT 16",
        )
        .unwrap()
        .query_map(
            params![committed.attempt_id.as_slice(), 37_i64, 40_i64],
            |row| row.get::<_, String>(3),
        )
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(plan.iter().any(|detail| detail.contains("PRIMARY KEY")));
    assert!(!plan.iter().any(|detail| detail == "SCAN attempt_chunks"));
    connection.close().unwrap();
}

#[test]
fn prior_epoch_recovery_preserves_only_the_committed_prefix() {
    let (_directory, root) = private_root("loxa-history-recovery-");
    let models = root.join("models");
    fs::create_dir(&models).unwrap();
    install_local_manifest(&models, "demo");
    let (mut connection, _) = open_store(&root).unwrap();
    let (_, committed) = admitted_attempt(&mut connection, &models, 23, 23);
    content::append_suffix(&mut connection, &suffix_input(&committed, 23, 0, "saved")).unwrap();

    assert!(recovery::recover_interrupted(&mut connection, "boot-2").unwrap());
    let state: (i64, i64, i64, Option<i64>, Option<i64>) = connection
        .query_row(
            "SELECT execution_outcome, save_outcome, saved_end, generated_end,
                    terminal_saved_end FROM attempts WHERE id = ?1",
            [committed.attempt_id.as_slice()],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(state, (4, 3, 5, None, None));
    assert_eq!(
        content::read_attempt_range(&connection, committed.attempt_id, 0, 5)
            .unwrap()
            .content,
        "saved"
    );
    assert!(!recovery::recover_interrupted(&mut connection, "boot-2").unwrap());

    admitted_attempt(&mut connection, &models, 24, 24);
    admitted_attempt(&mut connection, &models, 25, 25);
    assert_eq!(
        recovery::recover_interrupted(&mut connection, "boot-3")
            .unwrap_err()
            .kind(),
        HistoryErrorKind::Corrupt
    );
    let plan = connection
        .prepare(
            "EXPLAIN QUERY PLAN SELECT id FROM attempts
             WHERE execution_outcome = 0 OR save_outcome IN (0, 2)
             ORDER BY id LIMIT 2",
        )
        .unwrap()
        .query_map([], |row| row.get::<_, String>(3))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(plan
        .iter()
        .any(|detail| detail.contains("attempts_recovery")));
    connection.close().unwrap();
}

#[test]
fn recovery_rejects_an_unresolved_attempt_under_a_deleted_parent() {
    let (_directory, root) = private_root("loxa-history-recovery-parent-");
    let models = root.join("models");
    fs::create_dir(&models).unwrap();
    install_local_manifest(&models, "demo");
    let (mut connection, _) = open_store(&root).unwrap();
    let (conversation, _) = admitted_attempt(&mut connection, &models, 35, 35);
    connection
        .execute(
            "UPDATE conversations SET deleted = 1 WHERE id = ?1",
            [identity::decode_id(&conversation.id).unwrap().as_slice()],
        )
        .unwrap();
    assert_eq!(
        recovery::recover_interrupted(&mut connection, "boot-2")
            .unwrap_err()
            .kind(),
        HistoryErrorKind::Corrupt
    );
    connection.close().unwrap();
}

#[test]
fn tombstone_hides_immediately_and_purge_removes_bounded_content_batches() {
    let (_directory, root) = private_root("loxa-history-purge-");
    let models = root.join("models");
    fs::create_dir(&models).unwrap();
    install_local_manifest(&models, "demo");
    let (mut connection, _) = open_store(&root).unwrap();
    let (conversation, committed) = admitted_attempt(&mut connection, &models, 26, 26);
    for offset in 0..20 {
        content::append_suffix(&mut connection, &suffix_input(&committed, 26, offset, "x"))
            .unwrap();
    }
    content::finalize(
        &mut connection,
        &FinalizationInput {
            suffix: suffix_input(&committed, 26, 20, ""),
            execution_outcome: ExecutionOutcome::Completed,
            generated_end: 20,
            failure_code: None,
            statistics: None,
        },
    )
    .unwrap();
    for submission in 40..56 {
        let (_, unrelated) =
            admitted_attempt(&mut connection, &models, submission, i64::from(submission));
        content::finalize(
            &mut connection,
            &FinalizationInput {
                suffix: suffix_input(&unrelated, i64::from(submission), 0, ""),
                execution_outcome: ExecutionOutcome::Completed,
                generated_end: 0,
                failure_code: None,
                statistics: None,
            },
        )
        .unwrap();
    }
    let conversation_id = identity::decode_id(&conversation.id).unwrap();
    let mut details = Vec::new();
    for (sql, values) in [
        (
            purge::NEXT_ATTEMPT_SQL,
            vec![rusqlite::types::Value::Blob(conversation_id.to_vec())],
        ),
        (
            purge::DELETE_CHUNKS_SQL,
            vec![
                rusqlite::types::Value::Blob(committed.attempt_id.to_vec()),
                rusqlite::types::Value::Integer(16),
            ],
        ),
        (
            purge::DELETE_DRAFTS_SQL,
            vec![
                rusqlite::types::Value::Blob(conversation_id.to_vec()),
                rusqlite::types::Value::Integer(128),
            ],
        ),
        (
            purge::DELETE_TURNS_SQL,
            vec![
                rusqlite::types::Value::Blob(conversation_id.to_vec()),
                rusqlite::types::Value::Integer(128),
            ],
        ),
        (
            purge::DELETE_ATTEMPT_SQL,
            vec![rusqlite::types::Value::Blob(committed.attempt_id.to_vec())],
        ),
    ] {
        details.extend(
            connection
                .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
                .unwrap()
                .query_map(rusqlite::params_from_iter(values), |row| {
                    row.get::<_, String>(3)
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
        );
    }
    for index in [
        "turns_conversation",
        "attempts_latest",
        "attempts_prior",
        "drafts_conversation",
        "turns_selected_attempt",
    ] {
        assert!(
            details.iter().any(|detail| detail.contains(index)),
            "{details:?}"
        );
    }
    let reply = conversations::execute(
        &mut connection,
        &models,
        RuntimeIdentity::BundledB10344,
        WireCommand::DeleteConversation {
            conversation_id: conversation.id.clone(),
            expected_revision: "2".into(),
        },
    )
    .unwrap();
    let HistoryReply::ConversationDeleted { purge_complete, .. } = reply else {
        panic!("delete returned the wrong reply");
    };
    assert!(!purge_complete);
    assert_eq!(
        connection
            .query_row::<i64, _, _>("SELECT COUNT(*) FROM attempt_chunks", [], |row| row.get(0))
            .unwrap(),
        4
    );
    assert!(content::capture_attempt_prefix(&connection, committed.attempt_id).is_err());
    let mut batches = 1;
    while !conversations::resume_delete(&mut connection).unwrap() {
        batches += 1;
        assert!(batches < 16);
    }
    assert_eq!(batches, 3);
    assert_eq!(
        connection
            .query_row::<i64, _, _>("SELECT COUNT(*) FROM conversations", [], |row| row.get(0))
            .unwrap(),
        16
    );
    assert_eq!(
        connection
            .query_row::<i64, _, _>(
                "SELECT COUNT(*) FROM conversations WHERE id = ?1",
                [conversation_id.as_slice()],
                |row| row.get(0),
            )
            .unwrap(),
        0
    );
    connection.close().unwrap();
}

#[test]
fn deep_turn_seek_and_late_purge_keep_bounded_progress_with_128_terminal_turns() {
    let (_directory, root) = private_root("loxa-history-bounded-progress-");
    let models = root.join("models");
    fs::create_dir(&models).unwrap();
    install_local_manifest(&models, "demo");
    let (mut connection, _) = open_store(&root).unwrap();
    let conversation = create_local_conversation(&mut connection, &models);
    let conversation_id = identity::decode_id(&conversation.id).unwrap();

    for index in 0..128 {
        let marker = (index + 1) as u8;
        let generation = i64::from(marker);
        let committed = admission::admit_send(
            &mut connection,
            &prepared_admission(
                conversation_id,
                i64::from(index) + 1,
                marker,
                marker,
                generation,
                AdmissionKind::Send {
                    user_text: format!("question {index}"),
                    draft: None,
                },
            ),
        )
        .unwrap();
        content::finalize(
            &mut connection,
            &FinalizationInput {
                suffix: suffix_input(&committed, generation, 0, ""),
                execution_outcome: ExecutionOutcome::Completed,
                generated_end: 0,
                failure_code: None,
                statistics: None,
            },
        )
        .unwrap();
    }

    let callbacks = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&callbacks);
    connection
        .progress_handler(
            1,
            Some(move || {
                observed.fetch_add(1, Ordering::Relaxed);
                false
            }),
        )
        .unwrap();

    let before = callbacks.load(Ordering::Relaxed);
    let first = reads::list_turns(&connection, &conversation.id, None, 10).unwrap();
    let first_steps = callbacks.load(Ordering::Relaxed) - before;
    assert_eq!(first.turns.first().unwrap().ordinal, "128");
    assert_eq!(first.turns.last().unwrap().ordinal, "119");

    let before = callbacks.load(Ordering::Relaxed);
    let deep = reads::list_turns(
        &connection,
        &conversation.id,
        Some(loxa_ipc::TurnCursor {
            ordinal: "17".into(),
        }),
        10,
    )
    .unwrap();
    let deep_steps = callbacks.load(Ordering::Relaxed) - before;
    assert_eq!(deep.turns.first().unwrap().ordinal, "16");
    assert_eq!(deep.turns.last().unwrap().ordinal, "7");
    assert!(
        deep_steps <= first_steps.saturating_mul(3),
        "deep seek used {deep_steps} VM steps versus {first_steps} for the first page"
    );
    let plan = connection
        .prepare(&format!(
            "EXPLAIN QUERY PLAN {}",
            reads::LIST_TURNS_AFTER_SQL
        ))
        .unwrap()
        .query_map(
            rusqlite::params![conversation_id.as_slice(), 17_i64, 11_i64],
            |row| row.get::<_, String>(3),
        )
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(
        plan.iter().any(|detail| {
            detail.contains("SEARCH t USING INDEX")
                && detail.contains("conversation_id=? AND ordinal<?")
        }),
        "{plan:?}"
    );

    let reply = conversations::execute(
        &mut connection,
        &models,
        RuntimeIdentity::BundledB10344,
        WireCommand::DeleteConversation {
            conversation_id: conversation.id,
            expected_revision: "129".into(),
        },
    )
    .unwrap();
    assert!(matches!(
        reply,
        HistoryReply::ConversationDeleted {
            purge_complete: false,
            ..
        }
    ));

    let mut early_steps = None;
    let mut late_steps = None;
    loop {
        let attempts_before: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM attempts a JOIN turns t ON t.id = a.turn_id
                 WHERE t.conversation_id = ?1",
                [conversation_id.as_slice()],
                |row| row.get(0),
            )
            .unwrap();
        let empty_turns: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM turns t LEFT JOIN attempts a ON a.turn_id = t.id
                 WHERE t.conversation_id = ?1 AND a.id IS NULL",
                [conversation_id.as_slice()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(empty_turns, 0, "purge accumulated empty turn prefixes");
        if attempts_before == 0 {
            assert!(conversations::resume_delete(&mut connection).unwrap());
            break;
        }

        let before = callbacks.load(Ordering::Relaxed);
        assert!(!conversations::resume_delete(&mut connection).unwrap());
        let steps = callbacks.load(Ordering::Relaxed) - before;
        if attempts_before == 127 {
            early_steps = Some(steps);
        }
        if attempts_before == 1 {
            late_steps = Some(steps);
        }
        let turns_after: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM turns WHERE conversation_id = ?1",
                [conversation_id.as_slice()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(turns_after, attempts_before - 1);
    }
    let early_steps = early_steps.unwrap();
    let late_steps = late_steps.unwrap();
    assert!(
        late_steps <= early_steps.saturating_mul(3),
        "late purge used {late_steps} VM steps versus {early_steps} early"
    );
    connection
        .progress_handler(0, None::<fn() -> bool>)
        .unwrap();
    connection.close().unwrap();
}

#[test]
fn schema_one_fixture_migrates_atomically_to_schema_five() {
    let (_directory, root) = private_root("loxa-history-v1-migration-");
    let path = root.join("app.sqlite");
    schema::create_v1_fixture(&path);

    let (connection, info) = open_store(&root).unwrap();
    assert_eq!(info.schema_version, 5);
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(version, 5);
    let objects: Vec<String> = connection
        .prepare(
            "SELECT name FROM sqlite_schema
             WHERE type IN ('table', 'index') AND name NOT GLOB 'sqlite_*'
             ORDER BY name",
        )
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        objects,
        [
            "attempt_chunks",
            "attempt_finalizations",
            "attempt_sampling",
            "attempt_statistics",
            "attempts",
            "attempts_latest",
            "attempts_prior",
            "attempts_recovery",
            "attempts_unresolved",
            "conversation_sampling",
            "conversations",
            "conversations_recency",
            "drafts",
            "drafts_client_conversation",
            "drafts_client_unbound",
            "drafts_conversation",
            "turns",
            "turns_conversation",
            "turns_selected_attempt",
        ]
    );
    connection.close().unwrap();
}

#[test]
fn schema_two_fixture_migrates_atomically_to_schema_five() {
    let (_directory, root) = private_root("loxa-history-v2-migration-");
    let path = root.join("app.sqlite");
    schema::create_v2_fixture(&path);

    let (connection, info) = open_store(&root).unwrap();
    assert_eq!(info.schema_version, 5);
    assert_eq!(
        connection
            .query_row::<i64, _, _>("PRAGMA user_version", [], |row| row.get(0))
            .unwrap(),
        5
    );
    assert!(connection
        .prepare("SELECT attempt_id, start_offset, end_offset, content FROM attempt_chunks")
        .is_ok());
    assert!(connection
        .prepare("SELECT conversation_id, temperature, top_p FROM conversation_sampling")
        .is_ok());
    assert!(connection
        .prepare("SELECT attempt_id, temperature, top_p FROM attempt_sampling")
        .is_ok());
    assert!(connection
        .prepare("SELECT attempt_id, start_offset, end_offset FROM attempt_finalizations")
        .is_ok());
    assert!(connection
        .prepare(
            "SELECT attempt_id, qualified_input_tokens, qualified_output_tokens,
                    service_first_output_latency_ms,
                    qualified_engine_decode_tokens_per_second, service_total_duration_ms,
                    stop_reason FROM attempt_statistics",
        )
        .is_ok());
    connection.close().unwrap();
}

#[test]
fn populated_schema_fixtures_migrate_without_inventing_attempt_sampling() {
    for (schema_version, create_fixture) in [
        (3, schema::create_v3_fixture as fn(&Path)),
        (4, schema::create_v4_fixture as fn(&Path)),
    ] {
        let label = format!("loxa-history-v{schema_version}-to-v5-migration-");
        let (_directory, root) = private_root(&label);
        let path = root.join("app.sqlite");
        create_fixture(&path);

        let conversation_id = [1_u8; 16];
        let turn_id = [2_u8; 16];
        let attempt_id = [3_u8; 16];
        let mut fixture = Connection::open(&path).unwrap();
        fixture.pragma_update(None, "foreign_keys", true).unwrap();
        let transaction = fixture.transaction().unwrap();
        transaction
            .execute(
                "INSERT INTO conversations (
                 id, model_id, manifest_version, binding_profile,
                 primary_filename, primary_sha256, primary_size,
                 primary_source_kind, primary_source_filename,
                 title, system_instruction, max_output_tokens,
                 created_ms, updated_ms, revision, profile_revision, deleted
             ) VALUES (?1, 'demo', 2, 0, 'model.gguf', ?2, 4, 0,
                       'source.gguf', 'Migration', '', 512, 1, 2, 2, 1, 0)",
                params![conversation_id.as_slice(), [4_u8; 32].as_slice()],
            )
            .unwrap();
        transaction
            .execute(
                "INSERT INTO turns (id, conversation_id, ordinal, user_text, selected_attempt_id)
             VALUES (?1, ?2, 1, 'hello', NULL)",
                params![turn_id.as_slice(), conversation_id.as_slice()],
            )
            .unwrap();
        transaction
            .execute(
                "INSERT INTO attempts (
                 id, turn_id, attempt_number, submission_id, submission_hash,
                 admitted_conversation_revision, admitted_profile_revision,
                 prior_attempt_id, owner_epoch, operation_generation, model_id,
                 applied_engine_build, applied_engine_version, runtime_fingerprint,
                 effective_context, system_instruction, max_output_tokens, prompt_basis,
                 execution_outcome, save_outcome, saved_end, generated_end,
                 terminal_saved_end, failure_code, created_ms, updated_ms
             ) VALUES (
                 ?1, ?2, 1, ?3, ?4, 2, 1, NULL, 'owner', 1, 'demo',
                 'build', 'version', ?5, 4096, '', 512, ?6,
                 2, 1, 0, 0, 0, 'stopped', 1, 2
             )",
                params![
                    attempt_id.as_slice(),
                    turn_id.as_slice(),
                    [5_u8; 16].as_slice(),
                    [6_u8; 32].as_slice(),
                    b"fingerprint".as_slice(),
                    b"[]".as_slice(),
                ],
            )
            .unwrap();
        transaction
            .execute(
                "INSERT INTO attempt_finalizations (attempt_id, start_offset, end_offset)
             VALUES (?1, 0, 0)",
                params![attempt_id.as_slice()],
            )
            .unwrap();
        transaction
            .execute(
                "UPDATE turns SET selected_attempt_id = ?1 WHERE id = ?2",
                params![attempt_id.as_slice(), turn_id.as_slice()],
            )
            .unwrap();
        transaction.commit().unwrap();
        fixture.close().unwrap();

        let (connection, info) = open_store(&root).unwrap();
        assert_eq!(info.schema_version, 5);
        assert_eq!(
            connection
                .query_row::<i64, _, _>("PRAGMA user_version", [], |row| row.get(0))
                .unwrap(),
            5
        );
        assert!(connection
            .prepare(
                "SELECT attempt_id, qualified_input_tokens, qualified_output_tokens,
                    service_first_output_latency_ms,
                    qualified_engine_decode_tokens_per_second, service_total_duration_ms,
                    stop_reason FROM attempt_statistics",
            )
            .is_ok());
        let page = reads::list_turns(&connection, &identity::encode_id(conversation_id), None, 50)
            .unwrap();
        let attempt = page.turns[0].selected_attempt.as_ref().unwrap();
        assert_eq!(attempt.id, identity::encode_id(attempt_id));
        assert_eq!(attempt.execution, loxa_ipc::AttemptExecution::Stopped);
        assert_eq!(attempt.save, loxa_ipc::AttemptSave::Saved);
        assert_eq!(attempt.failure_code.as_deref(), Some("stopped"));
        assert_eq!(attempt.statistics, None);
        assert_eq!(attempt.effective_sampling, None);
        connection.close().unwrap();
    }
}

#[test]
fn failed_schema_three_migration_rolls_back_and_clean_retry_upgrades() {
    let (_directory, root) = private_root("loxa-history-v3-migration-rollback-");
    let path = root.join("app.sqlite");
    schema::create_v2_fixture(&path);
    let mut connection = Connection::open(&path).unwrap();

    assert!(schema::fail_v2_to_v3_after_recovery_index(&mut connection).is_err());
    assert_eq!(
        connection
            .query_row::<i64, _, _>("PRAGMA user_version", [], |row| row.get(0))
            .unwrap(),
        2
    );
    let objects = connection
        .prepare(
            "SELECT name FROM sqlite_schema
             WHERE type IN ('table', 'index') AND name NOT GLOB 'sqlite_*'
             ORDER BY name",
        )
        .unwrap()
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        objects,
        [
            "attempts",
            "attempts_latest",
            "attempts_unresolved",
            "conversations",
            "conversations_recency",
            "drafts",
            "drafts_client_conversation",
            "drafts_client_unbound",
            "turns",
        ]
    );
    connection.close().unwrap();

    let (connection, info) = open_store(&root).unwrap();
    assert_eq!(info.schema_version, 5);
    assert_eq!(
        connection
            .query_row::<i64, _, _>("PRAGMA user_version", [], |row| row.get(0))
            .unwrap(),
        5
    );
    assert!(connection
        .prepare("SELECT attempt_id, start_offset, end_offset, content FROM attempt_chunks")
        .is_ok());
    connection.close().unwrap();
}

#[test]
fn malformed_rows_are_rejected_before_large_values_are_copied() {
    let (_directory, root) = private_root("loxa-history-corrupt-row-");
    let models = root.join("models");
    fs::create_dir(&models).unwrap();
    install_local_manifest(&models, "demo");
    let (mut connection, _) = open_store(&root).unwrap();
    let conversation = create_local_conversation(&mut connection, &models);
    let client = identity::encode_id([8; 16]);
    let draft = match drafts::execute(
        &mut connection,
        DraftCommand::CreateScope {
            desktop_client_id: client.clone(),
            conversation_id: Some(conversation.id.clone()),
        },
    )
    .unwrap()
    {
        DraftReply::Snapshot(snapshot) => snapshot,
        _ => panic!("create scope returned the wrong reply"),
    };
    let legal = "x".repeat(loxa_ipc::MAX_DRAFT_TEXT_BYTES);
    let legal = match drafts::execute(
        &mut connection,
        DraftCommand::SaveSnapshot {
            draft_id: draft.id.clone(),
            desktop_client_id: client.clone(),
            revision: "1".into(),
            text: legal.clone(),
        },
    )
    .unwrap()
    {
        DraftReply::Snapshot(snapshot) => snapshot,
        _ => panic!("save returned the wrong reply"),
    };
    assert_eq!(legal.text.len(), loxa_ipc::MAX_DRAFT_TEXT_BYTES);

    connection
        .execute_batch("PRAGMA ignore_check_constraints = ON")
        .unwrap();
    connection
        .execute(
            "UPDATE conversations SET model_id = ?1 WHERE id = ?2",
            rusqlite::params![
                "m".repeat(40 * 1024),
                identity::decode_id(&conversation.id).unwrap().as_slice()
            ],
        )
        .unwrap();
    assert_eq!(
        conversations::execute(
            &mut connection,
            &models,
            RuntimeIdentity::BundledB10344,
            WireCommand::ListConversations {
                cursor: None,
                limit: 50,
            },
        )
        .unwrap_err()
        .kind(),
        HistoryErrorKind::Corrupt
    );
    connection
        .execute(
            "UPDATE conversations SET model_id = 'demo' WHERE id = ?1",
            [identity::decode_id(&conversation.id).unwrap().as_slice()],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE conversations SET title = ?1 WHERE id = ?2",
            rusqlite::params![
                "t".repeat(40 * 1024),
                identity::decode_id(&conversation.id).unwrap().as_slice()
            ],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE drafts SET text = ?1, text_hash = ?2 WHERE id = ?3",
            rusqlite::params![
                "d".repeat(128 * 1024),
                Sha256::digest(b"wrong").as_slice(),
                identity::decode_id(&draft.id).unwrap().as_slice(),
            ],
        )
        .unwrap();
    connection
        .execute_batch("PRAGMA ignore_check_constraints = OFF")
        .unwrap();

    assert_eq!(
        conversations::execute(
            &mut connection,
            &models,
            RuntimeIdentity::BundledB10344,
            WireCommand::ListConversations {
                cursor: None,
                limit: 50,
            },
        )
        .unwrap_err()
        .kind(),
        HistoryErrorKind::Corrupt
    );
    assert_eq!(
        drafts::execute(
            &mut connection,
            DraftCommand::ReadScope {
                draft_id: draft.id,
                desktop_client_id: client,
            },
        )
        .unwrap_err()
        .kind(),
        HistoryErrorKind::Corrupt
    );
    connection.close().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bounded_owner_keeps_stop_independent_of_stalled_sql_and_joins() {
    let (_directory, root) = private_root("loxa-history-owner-");
    let draining = Arc::new(AtomicBool::new(false));
    let owner = HistoryOwner::start(
        &root,
        root.join("models"),
        RuntimeIdentity::LegacyCliB10121,
        Arc::clone(&draining),
        "test-owner".into(),
    )
    .unwrap();
    let handle = owner.handle();
    let deadline = Instant::now() + Duration::from_secs(2);
    while handle.status().phase == HistoryPhase::Opening && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(handle.status().phase, HistoryPhase::Ready);

    let barrier = Arc::new(Barrier::new(2));
    handle.stall(Arc::clone(&barrier));
    barrier.wait();
    let mut requests = Vec::new();
    for _ in 0..8 {
        let handle = handle.clone();
        requests.push(tokio::spawn(async move {
            handle
                .execute(WireCommand::ListConversations {
                    cursor: None,
                    limit: 1,
                })
                .await
        }));
    }
    let capacity_deadline = Instant::now() + Duration::from_secs(1);
    while handle.ordinary.available_permits() != 0 && Instant::now() < capacity_deadline {
        tokio::task::yield_now().await;
    }
    assert_eq!(handle.ordinary.available_permits(), 0);
    let overflow = handle
        .execute(WireCommand::ListConversations {
            cursor: None,
            limit: 1,
        })
        .await
        .unwrap_err();
    assert_eq!(overflow.kind(), HistoryErrorKind::Busy);

    draining.store(true, std::sync::atomic::Ordering::Release);
    let rejected = handle
        .execute(WireCommand::ListConversations {
            cursor: None,
            limit: 1,
        })
        .await
        .unwrap_err();
    assert_eq!(rejected.kind(), HistoryErrorKind::WorkerUnavailable);
    handle.begin_drain();
    assert_eq!(handle.status().phase, HistoryPhase::FlushPending);
    let mut exit = handle.exit_receiver();
    barrier.wait();
    while *exit.borrow() == HistoryExit::Running {
        exit.changed().await.unwrap();
    }
    assert_eq!(*exit.borrow(), HistoryExit::Drained);
    for request in requests {
        match request.await.unwrap() {
            Ok(completion) => {
                if let Err(error) = completion.result {
                    assert_eq!(error.kind(), HistoryErrorKind::Interrupted);
                }
            }
            Err(error) => assert_eq!(error.kind(), HistoryErrorKind::WorkerUnavailable),
        }
    }
    owner.join().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persistence_owner_bounds_pending_bytes_and_orders_final_after_checkpoint() {
    let (_directory, root) = private_root("loxa-history-persistence-owner-");
    let models = root.join("models");
    fs::create_dir(&models).unwrap();
    install_local_manifest(&models, "demo");
    let owner = HistoryOwner::start(
        &root,
        models.clone(),
        RuntimeIdentity::BundledB10344,
        Arc::new(AtomicBool::new(false)),
        "boot-1".into(),
    )
    .unwrap();
    let handle = owner.handle();
    let deadline = Instant::now() + Duration::from_secs(2);
    while handle.status().phase == HistoryPhase::Opening && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(handle.status().phase, HistoryPhase::Ready);
    let created = handle
        .execute(WireCommand::CreateConversation {
            model_id: "demo".into(),
        })
        .await
        .unwrap();
    let conversation = match created.result.unwrap() {
        HistoryReply::Conversation(conversation) => conversation,
        _ => panic!("create returned the wrong reply"),
    };
    drop(created.permit);
    let prepared = Arc::new(prepared_admission(
        identity::decode_id(&conversation.id).unwrap(),
        1,
        31,
        31,
        31,
        AdmissionKind::Send {
            user_text: "question".into(),
            draft: None,
        },
    ));
    let admitted = handle.try_admit(prepared).unwrap().await.unwrap();
    let committed = admitted.result.unwrap();
    drop(admitted.permit);

    let barrier = Arc::new(Barrier::new(2));
    handle.stall(Arc::clone(&barrier));
    barrier.wait();
    let mut oversized_code = String::with_capacity(1024);
    oversized_code.push('x');
    assert_eq!(
        handle
            .try_finalize(Arc::new(FinalizationInput {
                suffix: suffix_input(&committed, 31, 0, ""),
                execution_outcome: ExecutionOutcome::Failed,
                generated_end: 0,
                failure_code: Some(oversized_code),
                statistics: None,
            }))
            .unwrap_err()
            .kind(),
        HistoryErrorKind::InvalidInput
    );
    assert_eq!(handle.persistence.available_permits(), 2);
    let checkpoint = Arc::new(suffix_input(&committed, 31, 0, "x"));
    let terminal = Arc::new(FinalizationInput {
        suffix: suffix_input(&committed, 31, 1, "y"),
        execution_outcome: ExecutionOutcome::Completed,
        generated_end: 2,
        failure_code: None,
        statistics: None,
    });
    let checkpoint_result = handle.try_append_suffix(Arc::clone(&checkpoint)).unwrap();
    let terminal_result = handle.try_finalize(Arc::clone(&terminal)).unwrap();
    assert_eq!(
        handle
            .try_append_suffix(Arc::new(suffix_input(&committed, 31, 0, "z")))
            .unwrap_err()
            .kind(),
        HistoryErrorKind::Busy
    );
    barrier.wait();
    let checkpoint_result = checkpoint_result.await.unwrap();
    assert_eq!(checkpoint_result.result.unwrap().end, 1);
    drop(checkpoint_result.permit);
    let terminal_result = terminal_result.await.unwrap();
    assert_eq!(terminal_result.result.unwrap().end, 2);
    drop(terminal_result.permit);

    handle.begin_drain();
    owner.join().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_suffix_result_reconciles_from_retained_shared_input() {
    let (_directory, root) = private_root("loxa-history-lost-suffix-");
    let models = root.join("models");
    fs::create_dir(&models).unwrap();
    install_local_manifest(&models, "demo");
    let owner = HistoryOwner::start(
        &root,
        models.clone(),
        RuntimeIdentity::BundledB10344,
        Arc::new(AtomicBool::new(false)),
        "boot-1".into(),
    )
    .unwrap();
    let handle = owner.handle();
    let deadline = Instant::now() + Duration::from_secs(2);
    while handle.status().phase == HistoryPhase::Opening && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let created = handle
        .execute(WireCommand::CreateConversation {
            model_id: "demo".into(),
        })
        .await
        .unwrap();
    let conversation = match created.result.unwrap() {
        HistoryReply::Conversation(conversation) => conversation,
        _ => panic!("create returned the wrong reply"),
    };
    drop(created.permit);
    let prepared = Arc::new(prepared_admission(
        identity::decode_id(&conversation.id).unwrap(),
        1,
        32,
        32,
        32,
        AdmissionKind::Send {
            user_text: "question".into(),
            draft: None,
        },
    ));
    let admitted = handle.try_admit(prepared).unwrap().await.unwrap();
    let committed = admitted.result.unwrap();
    drop(admitted.permit);

    let suffix = Arc::new(suffix_input(&committed, 32, 0, "durable"));
    handle.drop_next_persistence_reply();
    assert!(handle
        .try_append_suffix(Arc::clone(&suffix))
        .unwrap()
        .await
        .is_err());
    let reconciled = handle
        .try_append_suffix(Arc::clone(&suffix))
        .unwrap()
        .await
        .unwrap();
    assert_eq!(reconciled.result.unwrap().end, 7);
    drop(reconciled.permit);
    let finalization = Arc::new(FinalizationInput {
        suffix: suffix_input(&committed, 32, 7, ""),
        execution_outcome: ExecutionOutcome::Completed,
        generated_end: 7,
        failure_code: None,
        statistics: None,
    });
    handle.drop_next_persistence_reply();
    assert!(handle
        .try_finalize(Arc::clone(&finalization))
        .unwrap()
        .await
        .is_err());
    let reconciled_final = handle
        .try_finalize(Arc::clone(&finalization))
        .unwrap()
        .await
        .unwrap();
    assert_eq!(reconciled_final.result.unwrap().end, 7);
    drop(reconciled_final.permit);
    handle.begin_drain();
    owner.join().unwrap();
    assert_eq!(Arc::strong_count(&suffix), 1);
    assert_eq!(Arc::strong_count(&finalization), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_failure_retains_one_lifecycle_owner_and_allows_one_retry() {
    let (_directory, root) = private_root("loxa-history-close-retry-");
    let owner = HistoryOwner::start(
        &root,
        root.join("models"),
        RuntimeIdentity::LegacyCliB10121,
        Arc::new(AtomicBool::new(false)),
        "test-owner".into(),
    )
    .unwrap();
    let handle = owner.handle();
    let deadline = Instant::now() + Duration::from_secs(2);
    while handle.status().phase == HistoryPhase::Opening && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(handle.status().phase, HistoryPhase::Ready);

    handle.fail_next_close();
    handle.begin_drain();
    let deadline = Instant::now() + Duration::from_secs(2);
    while handle.status().phase != HistoryPhase::FlushFailed && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(handle.status().phase, HistoryPhase::FlushFailed);
    assert_eq!(handle.drain_dispatches(), 1);

    let barrier = Arc::new(Barrier::new(2));
    handle.stall(Arc::clone(&barrier));
    barrier.wait();
    for _ in 0..32 {
        handle.begin_drain();
    }
    assert_eq!(handle.status().phase, HistoryPhase::FlushPending);
    assert_eq!(handle.drain_dispatches(), 2);
    barrier.wait();
    let mut exit = handle.exit_receiver();
    while *exit.borrow() == HistoryExit::Running {
        exit.changed().await.unwrap();
    }
    assert_eq!(*exit.borrow(), HistoryExit::Drained);
    owner.join().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_open_schema_failure_retains_connection_until_explicit_drain() {
    let (_directory, root) = private_root("loxa-history-startup-failure-");
    let (connection, _) = open_store(&root).unwrap();
    connection
        .execute_batch("DROP INDEX conversations_recency")
        .unwrap();
    connection.close().unwrap();

    let owner = HistoryOwner::start(
        &root,
        root.join("models"),
        RuntimeIdentity::LegacyCliB10121,
        Arc::new(AtomicBool::new(false)),
        "test-owner".into(),
    )
    .unwrap();
    let handle = owner.handle();
    let deadline = Instant::now() + Duration::from_secs(2);
    while handle.status().phase == HistoryPhase::Opening && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(handle.status().phase, HistoryPhase::Unavailable);
    let mut exit = handle.exit_receiver();
    handle.begin_drain();
    while *exit.borrow() == HistoryExit::Running {
        exit.changed().await.unwrap();
    }
    assert_eq!(*exit.borrow(), HistoryExit::Drained);
    owner.join().unwrap();
}
