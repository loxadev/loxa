use crate::bootstrap::NodePaths;
use crate::control_state::recovery::{decide, RecoveryEvidence, UncertaintyReason};
use crate::control_state::repository::{
    arm_migration_statement_fault_for_test, arm_reconciliation_transaction_fault_for_test,
    ControlIdGenerator, DesiredKind, IntentReason, MigrationStatementFault, ReconciliationState,
    ReconciliationTransactionFault, RepositoryErrorClass, ScalarSource,
};
use crate::control_state::state_machine::test_support::storage::slice4_migration::{
    load_operation, unload_operation, V1Fixture,
};
use crate::control_state::state_machine::test_support::storage::TestRoot;
use crate::control_state::state_machine::{
    AdmissionRequest, DesiredDisposition, InstancePublication, MutationIds, Transition,
};
use crate::control_state::worker::{
    spawn_unpublished_from_repository_for_test, ControlStateError, ControlStateInit,
    ControlStateOpenInput, ControlStateWorker,
};
use crate::control_state::ControlRepository;
use crate::control_state::ControlStatePath;
use crate::runtime::NodeOwnerGuard;
use loxa_protocol::v2::{
    DecimalU64, EventId, OperationId, SlotId, StreamEpoch, V2NodeCapabilities, V2OperationError,
    V2OperationErrorCode, V2OperationProgress, V2OperationStatus,
};
use loxa_protocol::{NodeId, NodeInstanceId};
use std::path::PathBuf;
use std::str::FromStr;

const NODE_ID: &str = "81111111-1111-4111-8111-111111111111";
const SLOT_ID: &str = "82222222-2222-4222-8222-222222222222";
const EPOCH: &str = "83333333-3333-4333-8333-333333333333";
const INITIAL_EVENT: &str = "85555555-5555-4555-8555-555555555555";
const INSTANCE: &str = "84444444-4444-4444-8444-444444444444";

#[test]
fn every_migration_rollback_boundary_is_restart_resumable() {
    for completed_statements in 0..=11 {
        let fixture = super::storage::slice4_migration::V1Fixture::new(&format!(
            "recovery-rollback-{completed_statements}"
        ));
        let _fault = arm_migration_statement_fault_for_test(
            MigrationStatementFault::AfterStatement(completed_statements),
        );
        assert_eq!(
            fixture.reopen().unwrap_err(),
            RepositoryErrorClass::Durability
        );
        assert_eq!(fixture.raw_schema_version(), 1);
        let reopened = fixture.reopen().unwrap();
        assert_eq!(reopened.intent.desired_kind, DesiredKind::Unloaded);
        reopened.repository.close().unwrap();
    }
}

#[test]
fn slot_only_uncertainty_atomically_marks_intent_recovery() {
    let mut fixture = Fixture::unpublished("slot-intent-recovery");
    let before = fixture.repository().stored_slot_intent().unwrap();
    let decision = decide(RecoveryEvidence::uncertain(
        UncertaintyReason::OwnershipUnavailable,
    ));
    fixture
        .repository()
        .reconcile_slot_if_changed(&decision, 30, &mut MutationSequence::default())
        .unwrap()
        .expect("uncertain evidence must commit recovery");
    let intent = fixture.repository().stored_slot_intent().unwrap();
    assert_eq!(intent.desired_kind, before.desired_kind);
    assert_eq!(intent.desired_model_id, None);
    assert_eq!(intent.desired_revision, before.desired_revision);
    assert_eq!(intent.operation_id, None);
    assert_eq!(intent.reconciliation, ReconciliationState::RecoveryRequired);
    assert_eq!(intent.reason, Some(IntentReason::ChildEvidenceUncertain));
    fixture.repository().validate_all().unwrap();
}

struct InitialIds;
impl ControlIdGenerator for InitialIds {
    fn new_slot_id(&mut self) -> SlotId {
        SlotId::from_str(SLOT_ID).unwrap()
    }
    fn new_stream_epoch(&mut self) -> StreamEpoch {
        StreamEpoch::from_str(EPOCH).unwrap()
    }
    fn new_initial_event_id(&mut self) -> EventId {
        EventId::from_str(INITIAL_EVENT).unwrap()
    }
}

#[derive(Default)]
struct MutationSequence(u64);
impl MutationIds for MutationSequence {
    fn new_operation_id(&mut self) -> OperationId {
        self.0 += 1;
        OperationId::from_str(&format!("8aaaaaaa-0000-4000-8000-{:012x}", self.0)).unwrap()
    }
    fn new_event_id(&mut self) -> EventId {
        self.0 += 1;
        EventId::from_str(&format!("8bbbbbbb-0000-4000-8000-{:012x}", self.0)).unwrap()
    }
}

struct Fixture {
    _root: TestRoot,
    path: PathBuf,
    repository: Option<ControlRepository>,
}

impl Fixture {
    fn unpublished(label: &str) -> Self {
        let root = TestRoot::new(&format!("recovery-{label}"));
        let path = root.path().join("control-state.sqlite3");
        let repository = ControlRepository::open_or_migrate(
            &path,
            NodeId::from_str(NODE_ID).unwrap(),
            Some(ScalarSource::Fresh),
            &mut InitialIds,
        )
        .unwrap();
        Self {
            _root: root,
            path,
            repository: Some(repository),
        }
    }

    fn repository(&mut self) -> &mut ControlRepository {
        self.repository.as_mut().unwrap()
    }

    fn reopen(&mut self) {
        self.repository.take().unwrap().close().unwrap();
        self.repository = Some(
            ControlRepository::open_or_migrate(
                &self.path,
                NodeId::from_str(NODE_ID).unwrap(),
                None,
                &mut InitialIds,
            )
            .unwrap(),
        );
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(repository) = self.repository.take() {
            repository.close().unwrap();
        }
        let _ = std::fs::remove_file(&self.path);
        let _ = std::fs::remove_file(format!("{}.owner.lock", self.path.display()));
        let _ = std::fs::remove_file(format!("{}.migration.bak", self.path.display()));
        let _ = std::fs::remove_file(format!("{}.migration.bak.owner.lock", self.path.display()));
    }
}

#[test]
fn recovery_fixture_repository_parent_is_private() {
    let fixture = Fixture::unpublished("private-parent");
    super::storage::assert_private_repository_parent(&fixture.path);
}

fn capabilities(mask: u8) -> V2NodeCapabilities {
    V2NodeCapabilities {
        model_download: mask & 1 != 0,
        slot_load: mask & 2 != 0,
        slot_unload: mask & 4 != 0,
        operation_cancel: mask & 8 != 0,
        operation_stream: mask & 16 != 0,
    }
}

fn startup_fixture(label: &str) -> (PathBuf, PathBuf, ControlStateInit) {
    let root = std::env::temp_dir().join(format!(
        "loxa-startup-{label}-{}-{}",
        std::process::id(),
        StreamEpoch::new_v4()
    ));
    std::fs::create_dir_all(root.join("run/logs")).unwrap();
    let paths = NodePaths {
        models_dir: root.join("models"),
        state_path: root.join("run/managed.json"),
        logs_dir: root.join("run/logs"),
    };
    let baseline = loxa_core::supervisor::ManagedRun {
        schema_version: loxa_core::supervisor::RUNTIME_STATE_SCHEMA_VERSION,
        run_id: format!("startup-{label}"),
        model_id: None,
        owner_pid: std::process::id(),
        owner_process_start_time_unix_s: 1,
        stop_requested: false,
        lifecycle: loxa_core::supervisor::RunLifecycle::Unloaded,
        generation: 0,
        generation_alias: format!("loxa-startup-{label}-g0"),
        control_port: Some(19_431),
        port: 19_431,
        log_path: paths.logs_dir.join("owner.log"),
        child_pid: None,
        child_process_start_time_unix_s: None,
        child_pgid: None,
    };
    loxa_core::supervisor::create_unloaded_node_owner(&paths.state_path, baseline.clone()).unwrap();
    let state_path = paths.state_path.clone();
    let init = ControlStateInit {
        path: root.join("state/control-state.sqlite3").into(),
        node_id: NodeId::from_str(NODE_ID).unwrap(),
        open_input: ControlStateOpenInput {
            claimed_owner: NodeOwnerGuard::new(paths, baseline),
            first_migration_source: Some(ScalarSource::Fresh),
        },
        recovery_evidence: RecoveryEvidence::uncertain(UncertaintyReason::OwnershipUnavailable),
        now_unix_ms: 10,
    };
    (root, state_path, init)
}

fn existing_migration_startup_fixture(
    label: &str,
    control_path: &std::path::Path,
) -> (PathBuf, PathBuf, ControlStateInit) {
    let root = std::env::temp_dir().join(format!(
        "loxa-migration-startup-{label}-{}-{}",
        std::process::id(),
        StreamEpoch::new_v4()
    ));
    std::fs::create_dir_all(root.join("run/logs")).unwrap();
    let paths = NodePaths {
        models_dir: root.join("models"),
        state_path: root.join("run/managed.json"),
        logs_dir: root.join("run/logs"),
    };
    let baseline = loxa_core::supervisor::ManagedRun {
        schema_version: loxa_core::supervisor::RUNTIME_STATE_SCHEMA_VERSION,
        run_id: format!("migration-startup-{label}"),
        model_id: None,
        owner_pid: std::process::id(),
        owner_process_start_time_unix_s: 1,
        stop_requested: false,
        lifecycle: loxa_core::supervisor::RunLifecycle::Unloaded,
        generation: 0,
        generation_alias: format!("loxa-migration-startup-{label}-g0"),
        control_port: Some(19_431),
        port: 19_431,
        log_path: paths.logs_dir.join("owner.log"),
        child_pid: None,
        child_process_start_time_unix_s: None,
        child_pgid: None,
    };
    loxa_core::supervisor::create_unloaded_node_owner(&paths.state_path, baseline.clone()).unwrap();
    let state_path = paths.state_path.clone();
    let init = ControlStateInit {
        path: control_path.to_owned().into(),
        node_id: NodeId::from_str(NODE_ID).unwrap(),
        open_input: ControlStateOpenInput {
            claimed_owner: NodeOwnerGuard::new(paths, baseline),
            first_migration_source: None,
        },
        recovery_evidence: RecoveryEvidence::uncertain(UncertaintyReason::OwnershipUnavailable),
        now_unix_ms: 30,
    };
    (root, state_path, init)
}

#[derive(Clone, Copy)]
enum MigrationRecoveryStartupCase {
    MissingLoadingOperation,
    WrongKind,
    TerminalNullLoad,
    ActiveNullLoad,
    MissingUnloadOperation,
}

fn migration_recovery_startup_image(label: &str, case: MigrationRecoveryStartupCase) -> V1Fixture {
    let fixture = V1Fixture::new(label);
    match case {
        MigrationRecoveryStartupCase::MissingLoadingOperation => {
            fixture.set_v1_slot_loading(None, load_operation("target", 9));
            let connection = fixture.connection();
            connection.execute_batch("PRAGMA foreign_keys=OFF").unwrap();
            connection.execute("DELETE FROM operations", []).unwrap();
            connection.close().unwrap();
        }
        MigrationRecoveryStartupCase::WrongKind => {
            let mut operation = load_operation("target", 9);
            operation.kind = "download";
            fixture.set_v1_slot_loading(None, operation);
        }
        MigrationRecoveryStartupCase::TerminalNullLoad => {
            let mut operation = load_operation("target", 9);
            operation.status = "succeeded";
            operation.model_id = None;
            fixture.set_v1_slot_loading(None, operation);
        }
        MigrationRecoveryStartupCase::ActiveNullLoad => {
            let mut operation = load_operation("target", 9);
            operation.model_id = None;
            fixture.set_v1_slot_loading(None, operation);
        }
        MigrationRecoveryStartupCase::MissingUnloadOperation => {
            fixture.set_v1_slot("unloading", Some("observed"), Some(unload_operation(9)));
            let connection = fixture.connection();
            connection.execute_batch("PRAGMA foreign_keys=OFF").unwrap();
            connection.execute("DELETE FROM operations", []).unwrap();
            connection.close().unwrap();
        }
    }
    let opened = fixture.reopen().unwrap();
    assert!(matches!(
        opened.intent.reason,
        Some(IntentReason::MigrationAmbiguousLoading | IntentReason::MigrationOperationMismatch)
    ));
    opened.repository.validate_all().unwrap();
    opened.repository.close().unwrap();
    fixture
}

fn close_unexpected_startup(bootstrap: crate::control_state::worker::ControlStateBootstrap) {
    let crate::control_state::worker::ControlStateBootstrap {
        handle,
        worker,
        claimed_owner,
        ready_authority,
    } = bootstrap;
    drop(handle);
    drop(ready_authority);
    worker.join_for_test();
    drop(claimed_owner);
}

fn assert_migration_recovery_startup_gate(label: &str, case: MigrationRecoveryStartupCase) {
    let fixture = migration_recovery_startup_image(label, case);
    let before_bytes = std::fs::read(fixture.path()).unwrap();
    let before_logical = fixture.logical_snapshot();

    for attempt in 0..2 {
        let (owner_root, state_path, init) =
            existing_migration_startup_fixture(&format!("{label}-{attempt}"), fixture.path());
        match ControlStateWorker::open_reconcile_and_spawn(init) {
            Err(error) => assert_eq!(error, ControlStateError::DurableStateUnavailable),
            Ok(bootstrap) => {
                close_unexpected_startup(bootstrap);
                panic!("migration recovery must fail closed before worker startup")
            }
        }
        assert_managed_owner_released(&state_path);
        assert_eq!(std::fs::read(fixture.path()).unwrap(), before_bytes);
        assert_eq!(fixture.logical_snapshot(), before_logical);

        let reopened = fixture.reopen().unwrap();
        reopened.repository.validate_all().unwrap();
        reopened.repository.close().unwrap();
        assert_eq!(fixture.logical_snapshot(), before_logical);
        let _ = std::fs::remove_dir_all(owner_root);
    }
}

fn assert_safe_migration_recovery_is_consumed(label: &str, case: MigrationRecoveryStartupCase) {
    let fixture = migration_recovery_startup_image(label, case);
    let (owner_root, state_path, init) =
        existing_migration_startup_fixture(&format!("{label}-first"), fixture.path());
    let bootstrap = ControlStateWorker::open_reconcile_and_spawn(init).unwrap();
    let first_state = bootstrap.handle.snapshot();
    assert_eq!(
        first_state.slot.status,
        loxa_protocol::v2::V2SlotStatus::Recovery
    );
    assert!(first_state.operations.iter().all(|operation| !matches!(
        operation.status,
        V2OperationStatus::Queued | V2OperationStatus::Running | V2OperationStatus::Cancelling
    )));
    close_unexpected_startup(bootstrap);
    assert_managed_owner_released(&state_path);
    let after_first = fixture.logical_snapshot();
    let reopened = fixture.reopen().unwrap();
    reopened.repository.validate_all().unwrap();
    reopened.repository.close().unwrap();
    let _ = std::fs::remove_dir_all(owner_root);

    let (owner_root, state_path, init) =
        existing_migration_startup_fixture(&format!("{label}-repeat"), fixture.path());
    let bootstrap = ControlStateWorker::open_reconcile_and_spawn(init).unwrap();
    assert_eq!(*bootstrap.handle.snapshot(), *first_state);
    close_unexpected_startup(bootstrap);
    assert_managed_owner_released(&state_path);
    assert_eq!(fixture.logical_snapshot(), after_first);
    let _ = std::fs::remove_dir_all(owner_root);
}

#[test]
fn startup_consumes_safe_ambiguous_loading_migration_recovery_once() {
    assert_safe_migration_recovery_is_consumed(
        "startup-ambiguous-loading",
        MigrationRecoveryStartupCase::MissingLoadingOperation,
    );
}

#[test]
fn startup_consumes_safe_wrong_kind_migration_recovery_once() {
    assert_safe_migration_recovery_is_consumed(
        "startup-wrong-kind",
        MigrationRecoveryStartupCase::WrongKind,
    );
}

#[test]
fn startup_gate_preserves_terminal_null_load_migration_recovery() {
    assert_migration_recovery_startup_gate(
        "startup-terminal-null-load",
        MigrationRecoveryStartupCase::TerminalNullLoad,
    );
}

#[test]
fn startup_gate_preserves_active_null_load_migration_recovery() {
    assert_migration_recovery_startup_gate(
        "startup-active-null-load",
        MigrationRecoveryStartupCase::ActiveNullLoad,
    );
}

#[test]
fn startup_consumes_safe_missing_unload_migration_recovery_once() {
    assert_safe_migration_recovery_is_consumed(
        "startup-missing-unload",
        MigrationRecoveryStartupCase::MissingUnloadOperation,
    );
}

fn assert_managed_owner_released(state_path: &std::path::Path) {
    match loxa_core::supervisor::read_runtime_state(state_path).unwrap() {
        loxa_core::supervisor::RuntimeStateRead::Missing => {}
        loxa_core::supervisor::RuntimeStateRead::Loaded(runs) if runs.is_empty() => {}
        state => panic!("managed owner was not released: {state:?}"),
    }
}

#[test]
fn startup_panic_is_joined_and_classified_without_detaching_authority() {
    let (root, state_path, init) = startup_fixture("panic");
    let error = match ControlStateWorker::panic_during_initialization_for_test(init) {
        Ok(_) => panic!("startup panic must not initialize"),
        Err(error) => error,
    };
    assert_eq!(error, ControlStateError::WorkerPanicked);
    assert_managed_owner_released(&state_path);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn startup_timeout_reaps_after_bounded_stop_and_releases_authority() {
    let (root, state_path, init) = startup_fixture("timeout");
    let entered = std::sync::Arc::new(std::sync::Barrier::new(2));
    let release = std::sync::Arc::new(std::sync::Barrier::new(2));
    let worker_entered = std::sync::Arc::clone(&entered);
    let worker_release = std::sync::Arc::clone(&release);
    let startup = std::thread::spawn(move || {
        ControlStateWorker::block_after_durable_initialization_before_publication_for_test(
            init,
            worker_entered,
            worker_release,
        )
    });
    entered.wait();
    std::thread::sleep(std::time::Duration::from_millis(100));
    release.wait();
    let error = match startup.join().unwrap() {
        Ok(_) => panic!("timed out startup must not initialize"),
        Err(error) => error,
    };
    assert_eq!(error, ControlStateError::ShutdownDeadlineExceeded);
    assert_managed_owner_released(&state_path);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn existing_database_exact_absence_requires_core_capture_and_the_claimed_owner() {
    let root = std::env::temp_dir().join(format!(
        "loxa-existing-absence-{}-{}",
        std::process::id(),
        StreamEpoch::new_v4()
    ));
    std::fs::create_dir_all(root.join("run/logs")).unwrap();
    let paths = NodePaths {
        models_dir: root.join("models"),
        state_path: root.join("run/managed.json"),
        logs_dir: root.join("run/logs"),
    };
    let candidate = loxa_core::supervisor::ManagedRun {
        schema_version: loxa_core::supervisor::RUNTIME_STATE_SCHEMA_VERSION,
        run_id: "existing-db-owner".into(),
        model_id: None,
        owner_pid: std::process::id(),
        owner_process_start_time_unix_s: 1,
        stop_requested: false,
        lifecycle: loxa_core::supervisor::RunLifecycle::Unloaded,
        generation: 0,
        generation_alias: "loxa-existing-db-owner-g0".into(),
        control_port: Some(19_431),
        port: 19_431,
        log_path: paths.logs_dir.join("owner.log"),
        child_pid: None,
        child_process_start_time_unix_s: None,
        child_pgid: None,
    };
    std::fs::write(&paths.state_path, br#"{"schema_version":4,"runs":[]}"#).unwrap();
    let acquisition = loxa_core::supervisor::acquire_managed_owner(
        &paths.state_path,
        candidate,
        loxa_core::supervisor::ScalarCaptureMode::ExistingDatabase,
    )
    .unwrap();
    let guard = NodeOwnerGuard::new(paths, acquisition.claimed_run);
    assert!(matches!(
        crate::control_state::existing_database_absence_evidence(
            &guard,
            &acquisition.recovery_source,
        ),
        Ok(RecoveryEvidence::ExactAbsent(_))
    ));
    guard.finish().unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn acquisition_recovery_keeps_existing_database_exact_absence_exact() {
    let root = std::env::temp_dir().join(format!(
        "loxa-acquisition-exact-absence-{}-{}",
        std::process::id(),
        StreamEpoch::new_v4()
    ));
    std::fs::create_dir_all(root.join("run/logs")).unwrap();
    let paths = NodePaths {
        models_dir: root.join("models"),
        state_path: root.join("run/managed.json"),
        logs_dir: root.join("run/logs"),
    };
    let candidate = loxa_core::supervisor::ManagedRun {
        schema_version: loxa_core::supervisor::RUNTIME_STATE_SCHEMA_VERSION,
        run_id: "existing-db-exact-owner".into(),
        model_id: None,
        owner_pid: std::process::id(),
        owner_process_start_time_unix_s: 1,
        stop_requested: false,
        lifecycle: loxa_core::supervisor::RunLifecycle::Unloaded,
        generation: 0,
        generation_alias: "loxa-existing-db-exact-owner-g0".into(),
        control_port: Some(19_431),
        port: 19_431,
        log_path: paths.logs_dir.join("owner.log"),
        child_pid: None,
        child_process_start_time_unix_s: None,
        child_pgid: None,
    };
    std::fs::write(&paths.state_path, br#"{"schema_version":4,"runs":[]}"#).unwrap();
    let acquisition = loxa_core::supervisor::acquire_managed_owner(
        &paths.state_path,
        candidate,
        loxa_core::supervisor::ScalarCaptureMode::ExistingDatabase,
    )
    .unwrap();
    let (guard, scalar_source) = NodeOwnerGuard::from_acquisition(paths, acquisition);
    assert!(ScalarSource::from_managed(scalar_source).is_none());
    assert!(matches!(
        crate::control_state::acquisition_recovery_evidence(&guard, None),
        Ok(RecoveryEvidence::ExactAbsent(_))
    ));
    guard.finish().unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn acquisition_recovery_marks_a_dead_prior_ready_running_shape_uncertain() {
    let root = std::env::temp_dir().join(format!(
        "loxa-acquisition-dead-running-{}-{}",
        std::process::id(),
        StreamEpoch::new_v4()
    ));
    std::fs::create_dir_all(root.join("run/logs")).unwrap();
    let paths = NodePaths {
        models_dir: root.join("models"),
        state_path: root.join("run/managed.json"),
        logs_dir: root.join("run/logs"),
    };
    let mut dead_owner = std::process::Command::new("true").spawn().unwrap();
    let dead_owner_pid = dead_owner.id();
    dead_owner.wait().unwrap();
    let prior = loxa_core::supervisor::ManagedRun {
        schema_version: loxa_core::supervisor::RUNTIME_STATE_SCHEMA_VERSION,
        run_id: "prior-running-owner".into(),
        model_id: Some("ready-model".into()),
        owner_pid: dead_owner_pid,
        owner_process_start_time_unix_s: 1,
        stop_requested: false,
        lifecycle: loxa_core::supervisor::RunLifecycle::Running,
        generation: 3,
        generation_alias: "loxa-prior-running-owner-g3".into(),
        control_port: Some(19_430),
        port: 19_432,
        log_path: paths.logs_dir.join("prior.log"),
        child_pid: Some(dead_owner_pid),
        child_process_start_time_unix_s: Some(1),
        child_pgid: Some(i32::try_from(dead_owner_pid).unwrap()),
    };
    std::fs::write(
        &paths.state_path,
        serde_json::to_vec(&serde_json::json!({
            "schema_version": loxa_core::supervisor::RUNTIME_STATE_SCHEMA_VERSION,
            "runs": [&prior],
        }))
        .unwrap(),
    )
    .unwrap();
    let candidate = loxa_core::supervisor::ManagedRun {
        schema_version: loxa_core::supervisor::RUNTIME_STATE_SCHEMA_VERSION,
        run_id: "replacement-owner".into(),
        model_id: None,
        owner_pid: std::process::id(),
        owner_process_start_time_unix_s: 1,
        stop_requested: false,
        lifecycle: loxa_core::supervisor::RunLifecycle::Unloaded,
        generation: 0,
        generation_alias: "loxa-replacement-owner-g0".into(),
        control_port: Some(19_431),
        port: 19_431,
        log_path: paths.logs_dir.join("replacement.log"),
        child_pid: None,
        child_process_start_time_unix_s: None,
        child_pgid: None,
    };
    let acquisition = loxa_core::supervisor::acquire_managed_owner(
        &paths.state_path,
        candidate,
        loxa_core::supervisor::ScalarCaptureMode::ExistingDatabase,
    )
    .unwrap();
    let (guard, scalar_source) = NodeOwnerGuard::from_acquisition(paths.clone(), acquisition);
    assert!(ScalarSource::from_managed(scalar_source).is_none());
    assert_eq!(
        guard
            .acquisition_recovery()
            .and_then(loxa_core::supervisor::ManagedRecoverySource::prior_run),
        Some(&prior)
    );
    assert!(matches!(
        decide(
            crate::control_state::acquisition_recovery_evidence(&guard, None)
                .expect("dead prior running state must produce recovery evidence")
        ),
        crate::control_state::recovery::RecoveryDecision::Recovery { .. }
    ));
    guard.finish().unwrap();
    assert_eq!(
        loxa_core::supervisor::read_runtime_state(&paths.state_path).unwrap(),
        loxa_core::supervisor::RuntimeStateRead::Loaded(vec![prior])
    );
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn existing_database_missing_managed_state_is_not_exact_absence() {
    let root = std::env::temp_dir().join(format!(
        "loxa-existing-missing-{}-{}",
        std::process::id(),
        StreamEpoch::new_v4()
    ));
    std::fs::create_dir_all(root.join("run/logs")).unwrap();
    let paths = NodePaths {
        models_dir: root.join("models"),
        state_path: root.join("run/managed.json"),
        logs_dir: root.join("run/logs"),
    };
    let candidate = loxa_core::supervisor::ManagedRun {
        schema_version: loxa_core::supervisor::RUNTIME_STATE_SCHEMA_VERSION,
        run_id: "missing-existing-db-owner".into(),
        model_id: None,
        owner_pid: std::process::id(),
        owner_process_start_time_unix_s: 1,
        stop_requested: false,
        lifecycle: loxa_core::supervisor::RunLifecycle::Unloaded,
        generation: 0,
        generation_alias: "loxa-missing-existing-db-owner-g0".into(),
        control_port: Some(19_431),
        port: 19_431,
        log_path: paths.logs_dir.join("owner.log"),
        child_pid: None,
        child_process_start_time_unix_s: None,
        child_pgid: None,
    };
    let acquisition = loxa_core::supervisor::acquire_managed_owner(
        &paths.state_path,
        candidate,
        loxa_core::supervisor::ScalarCaptureMode::ExistingDatabase,
    )
    .unwrap();
    assert_eq!(
        acquisition.claimed_run.lifecycle,
        loxa_core::supervisor::RunLifecycle::RecoveryRequired
    );
    let (guard, scalar_source) = NodeOwnerGuard::from_acquisition(paths, acquisition);
    assert!(ScalarSource::from_managed(scalar_source).is_none());
    assert!(matches!(
        crate::control_state::acquisition_recovery_evidence(&guard, None),
        Ok(RecoveryEvidence::Uncertain(
            UncertaintyReason::LifecycleRecoveryRequired
        ))
    ));
    drop(guard);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn existing_database_absence_source_cannot_authorize_an_unrelated_claimed_owner() {
    let (root_b, _, init_b) = startup_fixture("absence-source-b");
    let ControlStateOpenInput {
        claimed_owner: guard_b,
        ..
    } = init_b.open_input;

    // A real core token is still insufficient unless it is bound to this guard.
    let root_c = std::env::temp_dir().join(format!(
        "loxa-source-mix-{}-{}",
        std::process::id(),
        StreamEpoch::new_v4()
    ));
    std::fs::create_dir_all(root_c.join("run/logs")).unwrap();
    let paths_c = NodePaths {
        models_dir: root_c.join("models"),
        state_path: root_c.join("run/managed.json"),
        logs_dir: root_c.join("run/logs"),
    };
    let mut candidate_c = guard_b.baseline().clone();
    candidate_c.run_id = "source-c".into();
    candidate_c.owner_process_start_time_unix_s = 3;
    candidate_c.generation_alias = "loxa-source-c-g0".into();
    std::fs::write(&paths_c.state_path, br#"{"schema_version":4,"runs":[]}"#).unwrap();
    let acquired_c = loxa_core::supervisor::acquire_managed_owner(
        &paths_c.state_path,
        candidate_c,
        loxa_core::supervisor::ScalarCaptureMode::ExistingDatabase,
    )
    .unwrap();
    assert!(crate::control_state::existing_database_absence_evidence(
        &guard_b,
        &acquired_c.recovery_source,
    )
    .is_err());
    drop(acquired_c);
    drop(guard_b);
    let _ = std::fs::remove_dir_all(root_b);
    let _ = std::fs::remove_dir_all(root_c);
}

#[tokio::test]
async fn publication_command_atomically_enables_the_worker_instance() {
    let mut fixture = Fixture::unpublished("worker-publication");
    let repository = fixture.repository.take().unwrap();
    let (handle, worker) = spawn_unpublished_from_repository_for_test(repository).unwrap();
    assert_eq!(
        handle
            .admit(AdmissionRequest::Download {
                model_id: "blocked-before-publication".into(),
                progress: V2OperationProgress {
                    completed_bytes: DecimalU64::new(0),
                    total_bytes: None,
                },
            })
            .await,
        Err(ControlStateError::DurableStateUnavailable)
    );
    handle
        .publish_instance(InstancePublication {
            node_instance_id: NodeInstanceId::from_str(INSTANCE).unwrap(),
            control_endpoint: "http://127.0.0.1:19431".into(),
            capabilities: capabilities(31),
            now_unix_ms: 10,
        })
        .await
        .unwrap();
    assert_eq!(
        handle
            .publish_instance(InstancePublication {
                node_instance_id: NodeInstanceId::new_v4(),
                control_endpoint: "http://127.0.0.1:19432".into(),
                capabilities: capabilities(0),
                now_unix_ms: 11,
            })
            .await,
        Err(ControlStateError::DurableStateUnavailable)
    );
    handle
        .admit(AdmissionRequest::Download {
            model_id: "enabled-after-publication".into(),
            progress: V2OperationProgress {
                completed_bytes: DecimalU64::new(0),
                total_bytes: None,
            },
        })
        .await
        .unwrap();
    drop(handle);
    worker.join_for_test();
}

#[tokio::test]
async fn production_initialization_owns_migration_reconciliation_and_publication() {
    let root = std::env::temp_dir().join(format!(
        "loxa-control-init-{}-{}",
        std::process::id(),
        StreamEpoch::new_v4()
    ));
    std::fs::create_dir_all(root.join("run/logs")).unwrap();
    let paths = NodePaths {
        models_dir: root.join("models"),
        state_path: root.join("run/managed.json"),
        logs_dir: root.join("run/logs"),
    };
    let baseline = loxa_core::supervisor::ManagedRun {
        schema_version: loxa_core::supervisor::RUNTIME_STATE_SCHEMA_VERSION,
        run_id: "task4-initial-owner".into(),
        model_id: None,
        owner_pid: std::process::id(),
        owner_process_start_time_unix_s: 1,
        stop_requested: false,
        lifecycle: loxa_core::supervisor::RunLifecycle::Unloaded,
        generation: 0,
        generation_alias: "loxa-task4-initial-owner-g0".into(),
        control_port: Some(19_431),
        port: 19_431,
        log_path: paths.logs_dir.join("owner.log"),
        child_pid: None,
        child_process_start_time_unix_s: None,
        child_pgid: None,
    };
    loxa_core::supervisor::create_unloaded_node_owner(&paths.state_path, baseline.clone()).unwrap();
    let control_path: ControlStatePath = root.join("state/control-state.sqlite3").into();
    let bootstrap = ControlStateWorker::open_reconcile_and_spawn(ControlStateInit {
        path: control_path,
        node_id: NodeId::from_str(NODE_ID).unwrap(),
        open_input: ControlStateOpenInput {
            claimed_owner: NodeOwnerGuard::new(paths.clone(), baseline),
            first_migration_source: Some(ScalarSource::Fresh),
        },
        recovery_evidence: RecoveryEvidence::uncertain(UncertaintyReason::OwnershipUnavailable),
        now_unix_ms: 10,
    })
    .unwrap();
    assert!(bootstrap.handle.snapshot().node.is_none());
    assert_eq!(
        bootstrap.handle.snapshot().slot.status,
        loxa_protocol::v2::V2SlotStatus::Unloaded
    );
    bootstrap
        .handle
        .publish_instance(InstancePublication {
            node_instance_id: NodeInstanceId::from_str(INSTANCE).unwrap(),
            control_endpoint: "http://127.0.0.1:19431".into(),
            capabilities: capabilities(31),
            now_unix_ms: 11,
        })
        .await
        .unwrap();
    bootstrap.worker.shutdown().await.unwrap();
    drop(bootstrap.handle);
    bootstrap.claimed_owner.finish().unwrap();
    assert!(bootstrap.ready_authority.is_none());
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn publish_instance_persists_all_five_capability_bits_and_one_full_event() {
    for mask in 0..32 {
        let mut fixture = Fixture::unpublished(&format!("capabilities-{mask}"));
        let receipt = fixture
            .repository()
            .publish_instance(
                InstancePublication {
                    node_instance_id: NodeInstanceId::from_str(INSTANCE).unwrap(),
                    control_endpoint: "http://127.0.0.1:19431".into(),
                    capabilities: capabilities(mask),
                    now_unix_ms: 100,
                },
                &mut MutationSequence::default(),
            )
            .unwrap();
        assert!(receipt.committed());
        fixture.repository().validate_all().unwrap();
        fixture.reopen();
        let state = fixture.repository().committed_state().unwrap();
        assert_eq!(state.node.unwrap().capabilities, capabilities(mask));
        assert_eq!(
            state
                .events
                .last()
                .unwrap()
                .node
                .as_ref()
                .unwrap()
                .capabilities,
            capabilities(mask)
        );
    }
}

#[test]
fn begin_stopping_is_atomic_idempotent_and_persists_one_full_node_event() {
    let mut fixture = Fixture::unpublished("begin-stopping");
    let instance = NodeInstanceId::from_str(INSTANCE).unwrap();
    let mut ids = MutationSequence::default();
    fixture
        .repository()
        .publish_instance(
            InstancePublication {
                node_instance_id: instance,
                control_endpoint: "http://127.0.0.1:19431".into(),
                capabilities: capabilities(31),
                now_unix_ms: 100,
            },
            &mut ids,
        )
        .unwrap();
    let running = fixture.repository().committed_state().unwrap();

    let committed = fixture
        .repository()
        .begin_stopping(instance, 101, &mut ids)
        .unwrap();
    assert!(committed.committed());
    let stopping = fixture.repository().committed_state().unwrap();
    assert_eq!(
        stopping.node.as_ref().unwrap().status,
        loxa_protocol::v2::V2NodeStatus::Stopping
    );
    assert_eq!(stopping.revision.get(), running.revision.get() + 1);
    assert_eq!(stopping.cursor.get(), running.cursor.get() + 1);
    assert_eq!(
        stopping.events.last().unwrap().node,
        stopping.node,
        "the committed node event must contain the complete stopping node"
    );

    let repeated = fixture
        .repository()
        .begin_stopping(instance, 102, &mut ids)
        .unwrap();
    assert!(repeated.is_noop());
    assert_eq!(fixture.repository().committed_state().unwrap(), stopping);
}

#[tokio::test]
async fn begin_stopping_worker_command_acknowledges_the_durable_commit() {
    let (root, _state_path, init) = startup_fixture("begin-stopping-worker");
    let bootstrap = ControlStateWorker::open_reconcile_and_spawn(init).unwrap();
    let instance = NodeInstanceId::from_str(INSTANCE).unwrap();
    bootstrap
        .handle
        .publish_instance(InstancePublication {
            node_instance_id: instance,
            control_endpoint: "http://127.0.0.1:19431".into(),
            capabilities: capabilities(31),
            now_unix_ms: 100,
        })
        .await
        .unwrap();

    let receipt = bootstrap.handle.begin_stopping(101).await.unwrap();
    assert!(receipt.committed());
    assert_eq!(
        bootstrap
            .handle
            .read_snapshot()
            .unwrap()
            .node
            .as_ref()
            .unwrap()
            .status,
        loxa_protocol::v2::V2NodeStatus::Stopping
    );
    assert!(bootstrap
        .handle
        .begin_stopping(102)
        .await
        .unwrap()
        .is_noop());

    bootstrap.worker.shutdown().await.unwrap();
    drop(bootstrap.handle);
    bootstrap.claimed_owner.finish().unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn restart_terminalizes_queued_running_and_cancelling_without_replay() {
    let mut fixture = Fixture::unpublished("operations");
    fixture
        .repository()
        .publish_instance(
            InstancePublication {
                node_instance_id: NodeInstanceId::from_str(INSTANCE).unwrap(),
                control_endpoint: "http://127.0.0.1:19431".into(),
                capabilities: capabilities(31),
                now_unix_ms: 100,
            },
            &mut MutationSequence::default(),
        )
        .unwrap();
    let mut ids = MutationSequence::default();
    let queued = fixture
        .repository()
        .admit(
            NodeInstanceId::from_str(INSTANCE).unwrap(),
            AdmissionRequest::Download {
                model_id: "queued".into(),
                progress: V2OperationProgress {
                    completed_bytes: DecimalU64::new(0),
                    total_bytes: None,
                },
            },
            101,
            &mut ids,
        )
        .unwrap()
        .operation_id;
    let running = fixture
        .repository()
        .admit(
            NodeInstanceId::from_str(INSTANCE).unwrap(),
            AdmissionRequest::Download {
                model_id: "running".into(),
                progress: V2OperationProgress {
                    completed_bytes: DecimalU64::new(0),
                    total_bytes: None,
                },
            },
            102,
            &mut ids,
        )
        .unwrap()
        .operation_id;
    fixture
        .repository()
        .observe(
            NodeInstanceId::from_str(INSTANCE).unwrap(),
            Transition::Started {
                operation_id: running,
                progress: Some(V2OperationProgress {
                    completed_bytes: DecimalU64::new(0),
                    total_bytes: None,
                }),
            },
            103,
            &mut ids,
        )
        .unwrap();
    let cancelling = fixture
        .repository()
        .admit(
            NodeInstanceId::from_str(INSTANCE).unwrap(),
            AdmissionRequest::Download {
                model_id: "cancelling".into(),
                progress: V2OperationProgress {
                    completed_bytes: DecimalU64::new(0),
                    total_bytes: None,
                },
            },
            104,
            &mut ids,
        )
        .unwrap()
        .operation_id;
    fixture
        .repository()
        .observe(
            NodeInstanceId::from_str(INSTANCE).unwrap(),
            Transition::Cancelling {
                operation_id: cancelling,
            },
            105,
            &mut ids,
        )
        .unwrap();

    let reconciled = fixture
        .repository()
        .reconcile_offline(
            RecoveryEvidence::uncertain(UncertaintyReason::OwnershipUnavailable),
            200,
            &mut ids,
        )
        .unwrap();
    assert_eq!(reconciled.receipts.len(), 4);
    let state = fixture.repository().committed_state().unwrap();
    for id in [queued, running, cancelling] {
        assert_eq!(
            state
                .operations
                .iter()
                .find(|operation| operation.operation_id == id)
                .unwrap()
                .status,
            V2OperationStatus::Failed
        );
    }
    assert!(!state.events.iter().rev().take(4).any(|event| event
        .operation
        .as_ref()
        .is_some_and(|operation| operation.status == V2OperationStatus::Running)));
}

#[test]
fn restart_terminalizes_operations_from_stopping_and_recovery_node_rows() {
    for node_status in ["stopping", "recovery"] {
        let mut fixture = Fixture::unpublished(&format!("node-{node_status}"));
        let mut ids = MutationSequence::default();
        fixture
            .repository()
            .publish_instance(
                InstancePublication {
                    node_instance_id: NodeInstanceId::from_str(INSTANCE).unwrap(),
                    control_endpoint: "http://127.0.0.1:19431".into(),
                    capabilities: capabilities(31),
                    now_unix_ms: 100,
                },
                &mut ids,
            )
            .unwrap();
        let operation_id = fixture
            .repository()
            .admit(
                NodeInstanceId::from_str(INSTANCE).unwrap(),
                AdmissionRequest::Download {
                    model_id: format!("interrupted-{node_status}"),
                    progress: V2OperationProgress {
                        completed_bytes: DecimalU64::new(0),
                        total_bytes: None,
                    },
                },
                101,
                &mut ids,
            )
            .unwrap()
            .operation_id;
        fixture
            .repository()
            .transaction(|tx| {
                tx.execute(
                    "UPDATE node_state SET status=?1 WHERE singleton=1",
                    [node_status],
                )?;
                Ok(())
            })
            .unwrap();
        fixture
            .repository()
            .reconcile_offline(
                RecoveryEvidence::uncertain(UncertaintyReason::LifecycleRecoveryRequired),
                200,
                &mut ids,
            )
            .unwrap();
        let status: String = fixture
            .repository()
            .read_transaction(|connection| {
                Ok(connection.query_row(
                    "SELECT status FROM operations WHERE operation_id=?1",
                    [operation_id.to_string()],
                    |row| row.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(status, "failed");
    }
}

#[test]
fn loading_plus_uncertain_recovery_emits_only_the_truthful_recovery_slot() {
    let mut fixture = Fixture::unpublished("loading-uncertain");
    let mut ids = MutationSequence::default();
    fixture
        .repository()
        .publish_instance(
            InstancePublication {
                node_instance_id: NodeInstanceId::from_str(INSTANCE).unwrap(),
                control_endpoint: "http://127.0.0.1:19431".into(),
                capabilities: capabilities(31),
                now_unix_ms: 100,
            },
            &mut ids,
        )
        .unwrap();
    let admission = fixture
        .repository()
        .admit(
            NodeInstanceId::from_str(INSTANCE).unwrap(),
            AdmissionRequest::Load {
                model_id: "model".into(),
            },
            101,
            &mut ids,
        )
        .unwrap();
    let operation_id = admission.operation_id;
    fixture
        .repository()
        .observe(
            NodeInstanceId::from_str(INSTANCE).unwrap(),
            Transition::Started {
                operation_id,
                progress: None,
            },
            102,
            &mut ids,
        )
        .unwrap();
    let before_cursor = fixture.repository().committed_state().unwrap().cursor;
    let reconciled = fixture
        .repository()
        .reconcile_offline(
            RecoveryEvidence::uncertain(UncertaintyReason::OwnershipUnavailable),
            200,
            &mut ids,
        )
        .unwrap();
    assert_eq!(reconciled.receipts.len(), 1);
    let state = fixture.repository().committed_state().unwrap();
    assert_eq!(state.slot.status, loxa_protocol::v2::V2SlotStatus::Recovery);
    assert_eq!(state.slot.model_id, None);
    assert_eq!(
        state.intent.desired,
        DesiredDisposition::Loaded("model".into())
    );
    assert_eq!(state.intent.desired_revision, admission.revision);
    assert_eq!(state.intent.operation_id, Some(operation_id));
    assert_eq!(
        state.intent.reconciliation,
        ReconciliationState::RecoveryRequired
    );
    fixture.reopen();
    fixture.repository().validate_all().unwrap();
    let appended: Vec<_> = state
        .events
        .iter()
        .filter(|event| event.sequence > before_cursor)
        .collect();
    assert_eq!(appended.len(), 1);
    assert_eq!(
        appended[0].slot.as_ref().unwrap().status,
        loxa_protocol::v2::V2SlotStatus::Recovery
    );
    assert_eq!(appended[0].slot.as_ref().unwrap().model_id, None);
}

#[test]
fn slot_only_restart_observation_is_not_attributed_to_the_prior_instance() {
    let mut fixture = Fixture::unpublished("slot-only-instance");
    let mut ids = MutationSequence::default();
    fixture
        .repository()
        .publish_instance(
            InstancePublication {
                node_instance_id: NodeInstanceId::from_str(INSTANCE).unwrap(),
                control_endpoint: "http://127.0.0.1:19431".into(),
                capabilities: capabilities(31),
                now_unix_ms: 100,
            },
            &mut ids,
        )
        .unwrap();
    let before_cursor = fixture.repository().committed_state().unwrap().cursor;
    fixture
        .repository()
        .reconcile_offline(
            RecoveryEvidence::uncertain(UncertaintyReason::OwnershipUnavailable),
            200,
            &mut ids,
        )
        .unwrap();
    let state = fixture.repository().committed_state().unwrap();
    let event = state
        .events
        .iter()
        .find(|event| event.sequence > before_cursor)
        .unwrap();
    assert_eq!(event.entity, loxa_protocol::v2::V2EventEntity::Slot);
    assert_eq!(event.node_instance_id, None);
}

#[test]
fn unloading_with_uncertain_authority_preserves_last_observed_model_without_ready_observation() {
    let mut fixture = Fixture::unpublished("unloading-absent");
    let mut ids = MutationSequence::default();
    fixture
        .repository()
        .publish_instance(
            InstancePublication {
                node_instance_id: NodeInstanceId::from_str(INSTANCE).unwrap(),
                control_endpoint: "http://127.0.0.1:19431".into(),
                capabilities: capabilities(31),
                now_unix_ms: 100,
            },
            &mut ids,
        )
        .unwrap();
    let load = fixture
        .repository()
        .admit(
            NodeInstanceId::from_str(INSTANCE).unwrap(),
            AdmissionRequest::Load {
                model_id: "model".into(),
            },
            101,
            &mut ids,
        )
        .unwrap()
        .operation_id;
    fixture
        .repository()
        .observe(
            NodeInstanceId::from_str(INSTANCE).unwrap(),
            Transition::Started {
                operation_id: load,
                progress: None,
            },
            102,
            &mut ids,
        )
        .unwrap();
    fixture
        .repository()
        .observe(
            NodeInstanceId::from_str(INSTANCE).unwrap(),
            Transition::Succeeded {
                operation_id: load,
                observed_model_id: Some("model".into()),
            },
            103,
            &mut ids,
        )
        .unwrap();
    let unload_admission = fixture
        .repository()
        .admit(
            NodeInstanceId::from_str(INSTANCE).unwrap(),
            AdmissionRequest::Unload,
            104,
            &mut ids,
        )
        .unwrap();
    let unload = unload_admission.operation_id;
    fixture
        .repository()
        .observe(
            NodeInstanceId::from_str(INSTANCE).unwrap(),
            Transition::Started {
                operation_id: unload,
                progress: None,
            },
            105,
            &mut ids,
        )
        .unwrap();
    let before_cursor = fixture.repository().committed_state().unwrap().cursor;
    let reconciled = fixture
        .repository()
        .reconcile_offline(
            RecoveryEvidence::uncertain(UncertaintyReason::OwnershipUnavailable),
            200,
            &mut ids,
        )
        .unwrap();
    assert_eq!(reconciled.receipts.len(), 1);
    let state = fixture.repository().committed_state().unwrap();
    assert_eq!(state.slot.status, loxa_protocol::v2::V2SlotStatus::Recovery);
    assert_eq!(state.slot.model_id.as_deref(), Some("model"));
    assert_eq!(state.intent.desired, DesiredDisposition::Unloaded);
    assert_eq!(state.intent.desired_revision, unload_admission.revision);
    assert_eq!(state.intent.operation_id, Some(unload));
    assert_eq!(
        state.intent.reconciliation,
        ReconciliationState::RecoveryRequired
    );
    fixture.reopen();
    fixture.repository().validate_all().unwrap();
    let appended: Vec<_> = state
        .events
        .iter()
        .filter(|event| event.sequence > before_cursor)
        .collect();
    assert_eq!(appended.len(), 1);
    assert_eq!(
        appended[0].slot.as_ref().unwrap().status,
        loxa_protocol::v2::V2SlotStatus::Recovery
    );
    assert!(!appended.iter().any(|event| event
        .slot
        .as_ref()
        .is_some_and(|slot| slot.status == loxa_protocol::v2::V2SlotStatus::Ready)));
}

#[test]
fn reconciliation_terminalizes_the_full_active_bound_and_is_resumable_idempotently() {
    let mut fixture = Fixture::unpublished("active-bound");
    let mut ids = MutationSequence::default();
    fixture
        .repository()
        .publish_instance(
            InstancePublication {
                node_instance_id: NodeInstanceId::from_str(INSTANCE).unwrap(),
                control_endpoint: "http://127.0.0.1:19431".into(),
                capabilities: capabilities(31),
                now_unix_ms: 100,
            },
            &mut ids,
        )
        .unwrap();
    for index in 0..128 {
        fixture
            .repository()
            .admit(
                NodeInstanceId::from_str(INSTANCE).unwrap(),
                AdmissionRequest::Download {
                    model_id: format!("interrupted-{index}"),
                    progress: V2OperationProgress {
                        completed_bytes: DecimalU64::new(0),
                        total_bytes: None,
                    },
                },
                101 + index,
                &mut ids,
            )
            .unwrap();
    }
    let first = fixture
        .repository()
        .reconcile_offline(
            RecoveryEvidence::uncertain(UncertaintyReason::OwnershipUnavailable),
            1_000,
            &mut ids,
        )
        .unwrap();
    assert_eq!(first.receipts.len(), 129);
    assert!(fixture
        .repository()
        .committed_state()
        .unwrap()
        .operations
        .iter()
        .all(|operation| operation.status == V2OperationStatus::Failed));
    let second = fixture
        .repository()
        .reconcile_offline(
            RecoveryEvidence::uncertain(UncertaintyReason::OwnershipUnavailable),
            1_001,
            &mut ids,
        )
        .unwrap();
    assert!(second.receipts.is_empty());
}

#[test]
fn every_reconciliation_transaction_fault_boundary_is_restart_resumable_and_fail_closed() {
    let mut fixture = Fixture::unpublished("commit-boundaries");
    let mut ids = MutationSequence::default();
    fixture
        .repository()
        .publish_instance(
            InstancePublication {
                node_instance_id: NodeInstanceId::from_str(INSTANCE).unwrap(),
                control_endpoint: "http://127.0.0.1:19431".into(),
                capabilities: capabilities(31),
                now_unix_ms: 100,
            },
            &mut ids,
        )
        .unwrap();
    let mut operations = Vec::new();
    for index in 0..128 {
        operations.push(
            fixture
                .repository()
                .admit(
                    NodeInstanceId::from_str(INSTANCE).unwrap(),
                    AdmissionRequest::Download {
                        model_id: format!("crash-boundary-{index}"),
                        progress: V2OperationProgress {
                            completed_bytes: DecimalU64::new(0),
                            total_bytes: None,
                        },
                    },
                    101 + index,
                    &mut ids,
                )
                .unwrap()
                .operation_id,
        );
    }

    let decision = decide(RecoveryEvidence::uncertain(
        UncertaintyReason::OwnershipUnavailable,
    ));
    for operation_id in operations {
        for fault in [
            ReconciliationTransactionFault::BeforeTransaction,
            ReconciliationTransactionFault::BeforeCommit,
        ] {
            arm_reconciliation_transaction_fault_for_test(fault);
            assert!(fixture
                .repository()
                .terminalize_interrupted_operation(
                    operation_id,
                    V2OperationError {
                        code: V2OperationErrorCode::NodeRestartedBeforeStart,
                        message: "node restarted before the operation started".into(),
                    },
                    &decision,
                    1_000,
                    &mut ids,
                )
                .is_err());
            fixture.reopen();
            let state = fixture.repository().committed_state().unwrap();
            assert_eq!(
                state
                    .operations
                    .iter()
                    .find(|operation| operation.operation_id == operation_id)
                    .unwrap()
                    .status,
                V2OperationStatus::Queued
            );
            assert!(!state
                .events
                .iter()
                .any(|event| event.operation.as_ref().is_some_and(|operation| {
                    operation.operation_id == operation_id
                        && operation.status == V2OperationStatus::Running
                })));
        }

        arm_reconciliation_transaction_fault_for_test(ReconciliationTransactionFault::AfterCommit);
        assert!(fixture
            .repository()
            .terminalize_interrupted_operation(
                operation_id,
                V2OperationError {
                    code: V2OperationErrorCode::NodeRestartedBeforeStart,
                    message: "node restarted before the operation started".into(),
                },
                &decision,
                1_000,
                &mut ids,
            )
            .is_err());
        fixture.reopen();
        let state = fixture.repository().committed_state().unwrap();
        assert_eq!(
            state
                .operations
                .iter()
                .find(|operation| operation.operation_id == operation_id)
                .unwrap()
                .status,
            V2OperationStatus::Failed
        );
        assert!(state
            .operations
            .iter()
            .all(|operation| operation.status != V2OperationStatus::Running));
    }

    for fault in [
        ReconciliationTransactionFault::BeforeTransaction,
        ReconciliationTransactionFault::BeforeCommit,
    ] {
        arm_reconciliation_transaction_fault_for_test(fault);
        assert!(fixture
            .repository()
            .reconcile_slot_if_changed(&decision, 1_001, &mut ids)
            .is_err());
        fixture.reopen();
        assert_eq!(
            fixture.repository().committed_state().unwrap().slot.status,
            loxa_protocol::v2::V2SlotStatus::Unloaded
        );
    }
    arm_reconciliation_transaction_fault_for_test(ReconciliationTransactionFault::AfterCommit);
    assert!(fixture
        .repository()
        .reconcile_slot_if_changed(&decision, 1_001, &mut ids)
        .is_err());
    fixture.reopen();
    assert_eq!(
        fixture.repository().committed_state().unwrap().slot.status,
        loxa_protocol::v2::V2SlotStatus::Recovery
    );
    let resumed = fixture
        .repository()
        .reconcile_offline(
            RecoveryEvidence::uncertain(UncertaintyReason::OwnershipUnavailable),
            1_002,
            &mut ids,
        )
        .unwrap();
    assert!(resumed.receipts.is_empty());
}

#[test]
fn identical_restart_slot_evidence_is_a_complete_noop() {
    let mut fixture = Fixture::unpublished("noop");
    let before = fixture.repository().committed_state().unwrap();
    let result = fixture
        .repository()
        .reconcile_offline(
            RecoveryEvidence::ExactAbsent(
                crate::control_state::recovery::ExactAbsenceProof::fresh_for_test(),
            ),
            200,
            &mut MutationSequence::default(),
        )
        .unwrap();
    assert!(result.receipts.is_empty());
    assert_eq!(fixture.repository().committed_state().unwrap(), before);
}

#[test]
fn existing_database_rejects_a_second_scalar_source_but_preserves_provenance() {
    let mut fixture = Fixture::unpublished("provenance");
    fixture.repository.take().unwrap().close().unwrap();
    let error = ControlRepository::open_or_migrate(
        &fixture.path,
        NodeId::from_str(NODE_ID).unwrap(),
        Some(ScalarSource::Fresh),
        &mut InitialIds,
    )
    .unwrap_err();
    assert_eq!(
        error.class(),
        crate::control_state::RepositoryErrorClass::Corrupt
    );
    fixture.repository = Some(
        ControlRepository::open_or_migrate(
            &fixture.path,
            NodeId::from_str(NODE_ID).unwrap(),
            None,
            &mut InitialIds,
        )
        .unwrap(),
    );
}

#[test]
fn absent_database_without_captured_scalar_source_fails_before_filesystem_mutation() {
    let root = std::env::temp_dir().join(format!(
        "loxa-missing-scalar-{}-{}",
        std::process::id(),
        StreamEpoch::new_v4()
    ));
    let path = root.join("control-state.sqlite3");
    let error = ControlRepository::open_or_migrate(
        &path,
        NodeId::from_str(NODE_ID).unwrap(),
        None,
        &mut InitialIds,
    )
    .unwrap_err();
    assert_eq!(
        error.class(),
        crate::control_state::RepositoryErrorClass::Corrupt
    );
    assert!(!root.exists());
}
