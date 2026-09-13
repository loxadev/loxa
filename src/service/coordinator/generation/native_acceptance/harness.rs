use super::{MODEL_ID, MODEL_SHA256, MODEL_SIZE};
use crate::paths::AppPaths;
use crate::service::coordinator::Coordinator;
use loxa_ipc::{ConnectMode, ReplyOutcome, RuntimePhase, ServiceClient, ServiceCommand};
use sha2::{Digest, Sha256};
use std::fs;
use std::os::unix::fs::{DirBuilderExt as _, FileTypeExt as _};
use std::path::{Path, PathBuf};
use std::time::Duration;

const WAIT_TIMEOUT: Duration = Duration::from_secs(60);
const FIXTURE_TEMP_PREFIX: &str = "lg-";

pub(super) struct NativeService {
    directory: Option<tempfile::TempDir>,
    pub(super) root: PathBuf,
    pub(super) coordinator: Coordinator,
    pub(super) client: ServiceClient,
    pub(super) runtime_evidence: NativeRuntimeEvidence,
    server: Option<tokio::task::JoinHandle<Result<(), String>>>,
}

pub(super) struct NativeRuntimeEvidence {
    pub(super) build: String,
    pub(super) commit: String,
    pub(super) version_line: String,
    pub(super) server_sha256: String,
    pub(super) server_size: u64,
    pub(super) inventory_sha256: String,
}

impl NativeService {
    pub(super) async fn start(app: &Path, source_model: &Path) -> Result<Self, String> {
        crate::verification::file::verify_regular(source_model, MODEL_SIZE, MODEL_SHA256)?;
        let directory = tempfile::Builder::new()
            .prefix(FIXTURE_TEMP_PREFIX)
            .tempdir_in("/private/tmp")
            .map_err(|error| error.to_string())?;
        let directory_path =
            fs::canonicalize(directory.path()).map_err(|error| error.to_string())?;
        let root = directory_path.join("dev");
        let forbidden_root = directory_path.join("normal");
        fs::create_dir(&forbidden_root).map_err(|error| error.to_string())?;
        let executable =
            fs::canonicalize(std::env::current_exe().map_err(|error| error.to_string())?)
                .map_err(|error| error.to_string())?;
        let bootstrap = loxa_ipc::initialize_development_root(
            &root,
            &forbidden_root,
            &executable,
            crate::service::BUILD_ID,
        )?;
        let paths = AppPaths::from_application_values(
            &app.join("Contents/MacOS/loxa-app"),
            Some(&root),
            None,
        )?;
        if paths.runtime_identity != crate::runtime_identity::RuntimeIdentity::BundledB10344 {
            return Err("native generation fixture did not select bundled b10344".into());
        }
        let runtime_evidence = inspect_bundled_runtime(&paths)?;
        let mut models = fs::DirBuilder::new();
        models.mode(0o700);
        models
            .create(&paths.models)
            .map_err(|error| error.to_string())?;
        install_model(&paths.models, source_model)?;
        let client = ServiceClient::load(&root, None, crate::service::BUILD_ID)?;
        let (machine_boot_id, boot_epoch) =
            crate::service::intent::boot_evidence(bootstrap.root().root_identity())?;
        let ownership = crate::runtime::RuntimeOwnership::acquire_service_unreconciled(&paths.run)
            .map_err(|error| match error {
                crate::runtime::RuntimeOwnershipAcquireError::Conflict => {
                    "isolated native generation runtime is already owned".into()
                }
                crate::runtime::RuntimeOwnershipAcquireError::Failed(error) => error,
            })?;
        let coordinator = Coordinator::start(
            paths,
            bootstrap.root().control_dir().to_owned(),
            bootstrap.root().root_identity().to_owned(),
            machine_boot_id,
            boot_epoch,
            ownership,
            tokio::runtime::Handle::current(),
            None,
            None,
        )?;
        let server = tokio::spawn(crate::service::server::run(
            bootstrap.clone(),
            coordinator.clone(),
        ));
        let mut fixture = Self {
            directory: Some(directory),
            root,
            coordinator,
            client,
            runtime_evidence,
            server: Some(server),
        };
        let initialization = async {
            fixture
                .wait_for_socket(bootstrap.root().socket_path())
                .await?;
            wait_for_history(&fixture.client).await?;
            load_model(&fixture.client).await
        }
        .await;
        if let Err(error) = initialization {
            let cleanup = fixture.shutdown(false).await;
            return match cleanup {
                Ok(()) => Err(error),
                Err(cleanup) => Err(format!("{error}; cleanup failed: {cleanup}")),
            };
        }
        Ok(fixture)
    }

    async fn wait_for_socket(&self, socket: &Path) -> Result<(), String> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            if socket.exists() {
                return Ok(());
            }
            if self
                .server
                .as_ref()
                .is_none_or(|server| server.is_finished())
            {
                return Err("native generation service stopped before binding".into());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err("native generation service did not bind its control socket".into());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    pub(super) async fn shutdown(&mut self, require_wire_stop: bool) -> Result<(), String> {
        let wire_stop = request_wire_stop(&self.client).await;
        let fallback = if wire_stop.is_err() {
            self.coordinator
                .stop_service()
                .map(|_| ())
                .map_err(|error| error.context)
        } else {
            Ok(())
        };
        let server_result = self.join_server().await;
        server_result?;
        if require_wire_stop {
            wire_stop
        } else {
            fallback
        }
    }

    async fn join_server(&mut self) -> Result<(), String> {
        let Some(mut server) = self.server.take() else {
            return Ok(());
        };
        match tokio::time::timeout(WAIT_TIMEOUT, &mut server).await {
            Ok(Ok(Ok(()))) => Ok(()),
            Ok(Ok(Err(error))) => self.resolve_server_failure(error).await,
            Ok(Err(error)) => {
                self.resolve_server_failure(format!(
                    "native generation server task failed: {error}"
                ))
                .await
            }
            Err(_) => {
                server.abort();
                let _ = server.await;
                self.resolve_server_failure("native generation service did not stop".into())
                    .await
            }
        }
    }

    async fn resolve_server_failure(&mut self, failure: String) -> Result<(), String> {
        self.coordinator.announce_server_stop();
        self.coordinator.drain_after_server_failure();
        if let Err(error) = tokio::time::timeout(
            WAIT_TIMEOUT,
            self.coordinator.wait_for_native_shutdown_resolution(),
        )
        .await
        .map_err(|_| "native generation owner resolution timed out".to_string())
        .and_then(|result| result)
        {
            let retained = self.retain_isolated_root();
            return Err(format!(
                "{failure}; {error}; retained isolated root at {retained}"
            ));
        }
        let coordinator = self.coordinator.clone();
        let joined = tokio::task::spawn_blocking(move || coordinator.join_owner()).await;
        match joined {
            Ok(Ok(())) => Err(failure),
            Ok(Err(error)) => {
                let retained = self.retain_isolated_root();
                Err(format!(
                    "{failure}; owner join failed: {error}; retained isolated root at {retained}"
                ))
            }
            Err(error) => {
                let retained = self.retain_isolated_root();
                Err(format!(
                    "{failure}; owner join task failed: {error}; retained isolated root at {retained}"
                ))
            }
        }
    }

    fn retain_isolated_root(&mut self) -> String {
        self.directory
            .take()
            .map(tempfile::TempDir::keep)
            .unwrap_or_else(|| self.root.clone())
            .display()
            .to_string()
    }
}

#[derive(serde::Deserialize)]
struct RuntimeInventory {
    runtime: RuntimeInventoryIdentity,
    regular_files: Vec<RuntimeInventoryFile>,
}

#[derive(serde::Deserialize)]
struct RuntimeInventoryIdentity {
    build: String,
    commit: String,
    version_line: String,
}

#[derive(serde::Deserialize)]
struct RuntimeInventoryFile {
    path: String,
    size: u64,
    sha256: String,
}

fn inspect_bundled_runtime(paths: &AppPaths) -> Result<NativeRuntimeEvidence, String> {
    const MAX_INVENTORY_BYTES: usize = 128 * 1024;

    let inventory_path = paths
        .runtime_inventory
        .as_deref()
        .ok_or_else(|| "native generation runtime inventory is absent".to_string())?;
    let bytes = crate::safe_file::read_regular_file_bounded(inventory_path, MAX_INVENTORY_BYTES)
        .map_err(|_| {
            "native generation runtime inventory is not a stable bounded file".to_string()
        })?;
    let inventory: RuntimeInventory = serde_json::from_slice(&bytes)
        .map_err(|_| "native generation runtime inventory is invalid".to_string())?;
    let mut servers = inventory
        .regular_files
        .into_iter()
        .filter(|file| file.path == "MacOS/llama-server");
    let server = servers
        .next()
        .ok_or_else(|| "native generation runtime inventory has no server".to_string())?;
    if servers.next().is_some() {
        return Err("native generation runtime inventory repeats the server".into());
    }
    crate::verification::file::verify_regular(&paths.managed_server, server.size, &server.sha256)?;
    Ok(NativeRuntimeEvidence {
        build: inventory.runtime.build,
        commit: inventory.runtime.commit,
        version_line: inventory.runtime.version_line,
        server_sha256: server.sha256,
        server_size: server.size,
        inventory_sha256: super::super::qualification_fixture::encode_digest(
            Sha256::digest(&bytes).into(),
        ),
    })
}

fn install_model(models: &Path, source_model: &Path) -> Result<(), String> {
    let manifest = super::super::qualification_fixture::manifest();
    let model_dir = models.join(MODEL_ID);
    let mut directory = fs::DirBuilder::new();
    directory.mode(0o700);
    directory
        .create(&model_dir)
        .map_err(|error| error.to_string())?;
    fs::copy(source_model, model_dir.join("model.gguf")).map_err(|error| error.to_string())?;
    crate::catalog::publish_manifest(models, &manifest)?;
    Ok(())
}

async fn wait_for_history(client: &ServiceClient) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let status = client
            .history_status(ConnectMode::ObserveExisting)
            .await
            .map_err(super::client::client_error)?;
        if status.is_ready() {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("native generation history did not become ready".into());
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

pub(super) async fn load_model(client: &ServiceClient) -> Result<(), String> {
    let accepted = match client
        .request(
            ConnectMode::ObserveExisting,
            ServiceCommand::Load {
                model_id: MODEL_ID.into(),
            },
        )
        .await
        .map_err(super::client::client_error)?
    {
        ReplyOutcome::Accepted(accepted) => accepted,
        _ => return Err("native generation load returned the wrong reply".into()),
    };
    let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
    loop {
        let status = runtime_status(client).await?;
        match status.phase {
            RuntimePhase::Ready {
                task_id,
                generation,
                model_id,
                ..
            } if task_id == accepted.task_id
                && generation == accepted.generation
                && model_id == MODEL_ID =>
            {
                return Ok(())
            }
            RuntimePhase::LoadFailed { category, .. } => {
                return Err(format!("native generation load failed: {category:?}"));
            }
            RuntimePhase::CleanupFailed { .. } | RuntimePhase::RecoveryRequired { .. } => {
                return Err("native generation load entered cleanup recovery".into());
            }
            _ if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            _ => return Err("native generation model did not become ready".into()),
        }
    }
}

async fn request_wire_stop(client: &ServiceClient) -> Result<(), String> {
    match client
        .request(ConnectMode::ObserveExisting, ServiceCommand::StopService)
        .await
        .map_err(super::client::client_error)?
    {
        ReplyOutcome::Accepted(_) => Ok(()),
        _ => Err("native generation StopService returned the wrong reply".into()),
    }
}

async fn runtime_status(client: &ServiceClient) -> Result<loxa_ipc::RuntimeStatus, String> {
    match client
        .request(ConnectMode::ObserveExisting, ServiceCommand::Status)
        .await
        .map_err(super::client::client_error)?
    {
        ReplyOutcome::Status(status) => Ok(status.runtime),
        _ => Err("native generation status returned the wrong reply".into()),
    }
}

pub(super) async fn wait_for_unloaded(client: &ServiceClient) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
    loop {
        match runtime_status(client).await?.phase {
            RuntimePhase::Unloaded => return Ok(()),
            RuntimePhase::CleanupFailed { .. } | RuntimePhase::RecoveryRequired { .. } => {
                return Err("stopped native engine cleanup failed".into());
            }
            _ if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            _ => return Err("stopped native engine did not become unloaded".into()),
        }
    }
}

pub(super) fn only_engine_endpoint(control_dir: &Path) -> Result<PathBuf, String> {
    let mut endpoint = None;
    for entry in fs::read_dir(control_dir).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let path = entry.path();
        if !path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("engine-") && name.ends_with(".sock"))
        {
            continue;
        }
        let metadata = fs::symlink_metadata(&path).map_err(|error| error.to_string())?;
        if !metadata.file_type().is_socket() {
            return Err("native generation engine endpoint is not a Unix socket".into());
        }
        if endpoint.replace(path).is_some() {
            return Err("native generation has more than one engine endpoint".into());
        }
    }
    let Some(endpoint) = endpoint else {
        return Err("native generation engine endpoint is absent".into());
    };
    Ok(endpoint)
}

#[test]
fn isolated_service_root_fits_the_socket_path_budget() {
    let directory = tempfile::Builder::new()
        .prefix(FIXTURE_TEMP_PREFIX)
        .tempdir_in("/private/tmp")
        .unwrap();
    let directory = fs::canonicalize(directory.path()).unwrap();
    let root = directory.join("dev");
    let forbidden_root = directory.join("normal");
    fs::create_dir(&forbidden_root).unwrap();
    let executable = fs::canonicalize(std::env::current_exe().unwrap()).unwrap();

    let bootstrap = loxa_ipc::initialize_development_root(
        &root,
        &forbidden_root,
        &executable,
        crate::service::BUILD_ID,
    )
    .unwrap();

    assert_eq!(bootstrap.root().control_dir(), root.join("run/service"));
}
