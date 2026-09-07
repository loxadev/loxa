mod coordinator;
mod dev_cli;
mod intent;
mod server;

use coordinator::Coordinator;
use loxa_ipc::{ClientBootstrap, ConnectMode, ReplyOutcome, ServiceClient, ServiceCommand};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

pub const BUILD_ID: &str = env!("CARGO_PKG_VERSION");

pub(crate) use dev_cli::run as run_development_cli;

pub enum HiddenServiceResult {
    NotServiceCommand,
    Exit(Result<i32, String>),
}

pub fn run_hidden_from_env() -> HiddenServiceResult {
    let mut arguments = std::env::args_os();
    let _program = arguments.next();
    let Some(command) = arguments.next() else {
        return HiddenServiceResult::NotServiceCommand;
    };
    if command != "__service-launch" && command != "__service-serve" {
        return HiddenServiceResult::NotServiceCommand;
    }
    let result = parse_hidden_invocation(arguments.collect()).and_then(|(root, root_identity)| {
        if command == "__service-launch" {
            launch_detached(&root, &root_identity)
        } else {
            serve(&root, &root_identity)
        }
    });
    HiddenServiceResult::Exit(result)
}

fn parse_hidden_invocation(arguments: Vec<OsString>) -> Result<(PathBuf, String), String> {
    match arguments.as_slice() {
        [root_flag, root, identity_flag, root_identity]
            if root_flag == "--data-root"
                && !root.is_empty()
                && identity_flag == "--root-identity"
                && !root_identity.is_empty() =>
        {
            let root_identity = root_identity
                .to_str()
                .ok_or_else(|| "invalid internal service invocation".to_string())?;
            Ok((PathBuf::from(root), root_identity.to_owned()))
        }
        _ => Err("invalid internal service invocation".into()),
    }
}

pub fn initialize_development_root(root: &Path) -> Result<ClientBootstrap, String> {
    let executable = std::env::current_exe().map_err(|error| error.to_string())?;
    initialize_development_root_from(root, &executable)
}

pub fn initialize_development_root_from(
    root: &Path,
    executable: &Path,
) -> Result<ClientBootstrap, String> {
    if !executable.is_absolute() {
        return Err("service development origin must be absolute".into());
    }
    let paths = crate::paths::AppPaths::from_env()?;
    loxa_ipc::initialize_development_root(root, &paths.root, executable, BUILD_ID)
}

pub async fn development_request(
    root: &Path,
    mode: ConnectMode,
    command: ServiceCommand,
) -> Result<ReplyOutcome, String> {
    let normal = crate::paths::AppPaths::from_env()?;
    let client = ServiceClient::load_async(root, Some(&normal.root), BUILD_ID).await?;
    client
        .request(mode, command)
        .await
        .map_err(|error| error.to_string())
}

fn launch_detached(root: &Path, expected_root_identity: &str) -> Result<i32, String> {
    let normal = crate::paths::AppPaths::from_env()?;
    let bootstrap =
        ClientBootstrap::load_expected(root, Some(&normal.root), expected_root_identity)?;
    let executable = std::env::current_exe().map_err(|error| error.to_string())?;
    close_inherited_descriptors()?;
    bootstrap.root().validate_current()?;
    let mut command = Command::new(executable);
    command
        .arg("__service-serve")
        .arg("--data-root")
        .arg(root)
        .arg("--root-identity")
        .arg(expected_root_identity)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        // SAFETY: setsid and umask are async-signal-safe and access no captured
        // Rust state in the post-fork child.
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                libc::umask(0o077);
                Ok(())
            });
        }
    }
    command
        .spawn()
        .map_err(|error| format!("could not detach Loxa background service: {error}"))?;
    Ok(0)
}

#[cfg(target_os = "linux")]
fn close_inherited_descriptors() -> Result<(), String> {
    // The launcher is a dedicated process and has not created the Command
    // spawn error pipe yet, so closing every inherited descriptor here cannot
    // interfere with Rust's exec error reporting.
    if unsafe { libc::close_range(3, u32::MAX, 0) } == 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    if !matches!(error.raw_os_error(), Some(code) if code == libc::ENOSYS || code == libc::EINVAL) {
        return Err(format!(
            "could not close inherited service descriptors: {error}"
        ));
    }
    close_inherited_descriptors_from("/proc/self/fd")
}

#[cfg(not(target_os = "linux"))]
fn close_inherited_descriptors() -> Result<(), String> {
    // `/dev/fd` is a kernel view of the descriptors that are actually open,
    // including descriptors above a lowered soft RLIMIT_NOFILE. Collect the
    // finite snapshot and drop its directory descriptor before closing it.
    close_inherited_descriptors_from("/dev/fd")
}

fn close_inherited_descriptors_from(directory: &str) -> Result<(), String> {
    let descriptors = std::fs::read_dir(directory)
        .map_err(|error| format!("could not enumerate inherited service descriptors: {error}"))?
        .map(|entry| {
            entry
                .map_err(|error| error.to_string())?
                .file_name()
                .to_str()
                .ok_or_else(|| "invalid descriptor inventory entry".to_string())?
                .parse::<libc::c_int>()
                .map_err(|_| "invalid descriptor inventory entry".to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    for descriptor in descriptors
        .into_iter()
        .filter(|descriptor| *descriptor >= 3)
    {
        if unsafe { libc::fcntl(descriptor, libc::F_GETFD) } == -1 {
            if std::io::Error::last_os_error().raw_os_error() == Some(libc::EBADF) {
                continue;
            }
            return Err("could not inspect an inherited service descriptor".into());
        }
        if unsafe { libc::close(descriptor) } != 0 {
            return Err("could not close an inherited service descriptor".into());
        }
    }
    Ok(())
}

fn serve(root: &Path, expected_root_identity: &str) -> Result<i32, String> {
    // Service startup precedes runtime threads, so this process-wide setting is
    // deterministic for the control socket and engine descendants.
    #[cfg(unix)]
    unsafe {
        libc::umask(0o077);
    }
    let normal = crate::paths::AppPaths::from_env()?;
    let bootstrap =
        ClientBootstrap::load_expected(root, Some(&normal.root), expected_root_identity)?;
    bootstrap.origin().validate_current()?;
    let _instance = bootstrap.root().acquire_instance()?;
    bootstrap.root().validate_current()?;
    let (machine_boot_id, boot_epoch) = intent::boot_evidence(bootstrap.root().root_identity())?;
    let ownership = crate::runtime::RuntimeOwnership::acquire_service_unreconciled(
        &bootstrap.root().root().join("run"),
    )
    .map_err(runtime_owner_error)?;
    let initial_recovery = intent::audit_retained(
        bootstrap.root().control_dir(),
        bootstrap.root().root_identity(),
    )
    .err()
    .or_else(|| {
        ownership
            .audit_clean_for_service()
            .err()
            .map(runtime_owner_error)
    });
    let paths = crate::paths::AppPaths::from_application_values(
        bootstrap.origin().executable(),
        Some(bootstrap.root().root()),
        std::env::var_os("HOME").as_deref().map(Path::new),
    )?;
    // Logging failure must not decide whether the background service can own
    // or clean its runtime. A successful guard lives through server shutdown.
    let diagnostics =
        match loxa_diagnostics::init(&paths.logs, loxa_diagnostics::ProcessRole::Service) {
            Ok(diagnostics) => Some(diagnostics),
            Err(_) => {
                eprintln!("Loxa background-service diagnostics are unavailable");
                None
            }
        };
    tracing::info!(event = "service_startup");
    let diagnostics_health = diagnostics
        .as_ref()
        .map(loxa_diagnostics::Diagnostics::health_handle);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .thread_name("loxa-service-io")
        .build()
        .map_err(|error| error.to_string())?;
    let coordinator = Coordinator::start(
        paths,
        bootstrap.root().control_dir().to_owned(),
        bootstrap.root().root_identity().to_owned(),
        machine_boot_id,
        boot_epoch,
        ownership,
        runtime.handle().clone(),
        initial_recovery,
        diagnostics_health,
    )?;
    let server_result = runtime.block_on(server::run(bootstrap, coordinator));
    tracing::info!(event = "service_shutdown");
    if diagnostics
        .map(loxa_diagnostics::Diagnostics::finish)
        .is_some_and(|health| !health.is_healthy())
    {
        eprintln!("Some Loxa background-service diagnostics could not be retained");
    }
    server_result?;
    Ok(0)
}

fn runtime_owner_error(error: crate::runtime::RuntimeOwnershipAcquireError) -> String {
    match error {
        crate::runtime::RuntimeOwnershipAcquireError::Conflict => {
            "another Loxa runtime owns the development root".into()
        }
        crate::runtime::RuntimeOwnershipAcquireError::Failed(message) => message,
    }
}
