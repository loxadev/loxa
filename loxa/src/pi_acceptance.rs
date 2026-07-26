use loxa_core::engine::llama_cpp::QUALIFIED_LLAMA_CPP_RUNTIME_IDENTITY;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(unix)]
use std::os::unix::process::CommandExt as _;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};
use tempfile::{Builder, NamedTempFile};
use url::Url;

const GATEWAY_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_GATEWAY_BYTES: u64 = 64 * 1024;
const MAX_BRIDGE_BYTES: usize = 64 * 1024;
const PI_PROCESS_TIMEOUT_MS: u64 = 120_000;
const BRIDGE_OUTER_TIMEOUT: Duration = Duration::from_millis(PI_PROCESS_TIMEOUT_MS + 7_000);
const BRIDGE_CANCEL_GRACE: Duration = Duration::from_secs(1);
const INDEPENDENT_VERIFIER_TIMEOUT: Duration = Duration::from_secs(10);
const PI_BRIDGE_SOURCE: &[u8] = include_bytes!("../../scripts/pi-acceptance.mjs");
const PI_ACCEPTANCE_EXTENSION: &[u8] =
    include_bytes!("../../examples/pi/tool-loop/acceptance-gate.mjs");
const PI_INDEPENDENT_VERIFIER: &str =
    include_str!("../../examples/pi/tool-loop/independent-verify.mjs");
const PI_ACCEPTANCE_PROMPT: &str = include_str!("../../examples/pi/tool-loop/prompt.txt");
const PI_FIXTURE_PACKAGE: &[u8] = include_bytes!("../../examples/pi/tool-loop/seed/package.json");
const PI_FIXTURE_SOURCE: &[u8] =
    include_bytes!("../../examples/pi/tool-loop/seed/src/merge-ranges.mjs");
const PI_FIXTURE_VERIFIER: &[u8] =
    include_bytes!("../../examples/pi/tool-loop/seed/test/verify.mjs");
#[cfg(test)]
const PI_EXPECTED_SOURCE: &[u8] =
    include_bytes!("../../examples/pi/tool-loop/expected/src/merge-ranges.mjs");

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum AcceptancePhase {
    MacLocal,
    WindowsTailnet,
    PostRecovery,
}

impl AcceptancePhase {
    pub(crate) fn parse(value: &str) -> io::Result<Self> {
        match value {
            "mac-local" => Ok(Self::MacLocal),
            "windows-tailnet" => Ok(Self::WindowsTailnet),
            "post-recovery" => Ok(Self::PostRecovery),
            _ => Err(invalid(
                "phase must be mac-local, windows-tailnet, or post-recovery",
            )),
        }
    }
}

#[derive(Debug)]
pub(crate) struct PiAcceptanceRequest {
    pub(crate) phase: AcceptancePhase,
    pub(crate) base_url: String,
    pub(crate) pi_entrypoint: PathBuf,
    pub(crate) max_tokens: u16,
    pub(crate) expected_config_sha256: Option<String>,
    pub(crate) evidence_dir: PathBuf,
}

#[derive(Debug)]
pub(crate) struct BridgeInvocation {
    pub(crate) pi_entrypoint: PathBuf,
    pub(crate) extension: PathBuf,
    pub(crate) prompt: String,
    pub(crate) workspace: PathBuf,
    pub(crate) environment: BTreeMap<String, OsString>,
}

pub(crate) trait AcceptanceRuntime {
    fn gateway_json(&mut self, url: &str) -> io::Result<Vec<u8>>;
    fn run_bridge(&mut self, invocation: &BridgeInvocation) -> io::Result<Vec<u8>>;
}

struct LiveRuntime {
    client: reqwest::blocking::Client,
    bridge: PathBuf,
    _bridge_directory: tempfile::TempDir,
}

impl LiveRuntime {
    fn new() -> io::Result<Self> {
        let client = reqwest::blocking::Client::builder()
            .timeout(GATEWAY_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(io::Error::other)?;
        let bridge_directory = Builder::new().prefix("loxa-pi-bridge-").tempdir()?;
        let bridge = bridge_directory.path().join("pi-acceptance.mjs");
        write_private_file(&bridge, PI_BRIDGE_SOURCE)?;
        Ok(Self {
            client,
            bridge,
            _bridge_directory: bridge_directory,
        })
    }
}

impl AcceptanceRuntime for LiveRuntime {
    fn gateway_json(&mut self, url: &str) -> io::Result<Vec<u8>> {
        let response = self
            .client
            .get(url)
            .send()
            .map_err(|_| io::Error::other("gateway acceptance request failed"))?;
        if !response.status().is_success() {
            return Err(io::Error::other(
                "gateway acceptance endpoint was unavailable",
            ));
        }
        let mut bytes = Vec::new();
        response
            .take(MAX_GATEWAY_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| io::Error::other("gateway acceptance response was invalid"))?;
        if bytes.len() as u64 > MAX_GATEWAY_BYTES {
            return Err(io::Error::other(
                "gateway acceptance response exceeded its size limit",
            ));
        }
        Ok(bytes)
    }

    fn run_bridge(&mut self, invocation: &BridgeInvocation) -> io::Result<Vec<u8>> {
        let mut command = Command::new("node");
        command
            .arg(&self.bridge)
            .arg("--pi-entrypoint")
            .arg(&invocation.pi_entrypoint)
            .arg("--extension")
            .arg(&invocation.extension)
            .arg("--prompt")
            .arg(&invocation.prompt)
            .arg("--timeout-ms")
            .arg(PI_PROCESS_TIMEOUT_MS.to_string())
            .current_dir(&invocation.workspace)
            .env_clear()
            .envs(&invocation.environment)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        #[cfg(unix)]
        command.process_group(0);
        let child = command
            .spawn()
            .map_err(|_| io::Error::other("Pi bridge failed to start"))?;
        let output = wait_for_bridge(child, BRIDGE_OUTER_TIMEOUT)?;
        if !output.status.success() {
            return Err(io::Error::other("Pi bridge exited unsuccessfully"));
        }
        if output.stdout.len() > MAX_BRIDGE_BYTES || output.stderr.len() > MAX_BRIDGE_BYTES {
            return Err(io::Error::other("Pi bridge output exceeded its size limit"));
        }
        Ok(output.stdout)
    }
}

#[derive(Debug)]
struct BridgeOutput {
    status: std::process::ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

fn wait_for_bridge(child: std::process::Child, timeout: Duration) -> io::Result<BridgeOutput> {
    let deadline = Instant::now() + timeout;
    let cancellation_grace = BRIDGE_CANCEL_GRACE.min(timeout / 2);
    let process_deadline = deadline.checked_sub(cancellation_grace).unwrap_or(deadline);
    let mut child = BridgeChildGuard::new(child, cancellation_grace);
    let stdout = child
        .child_mut()
        .stdout
        .take()
        .ok_or_else(|| io::Error::other("Pi bridge stdout was unavailable"))?;
    let stderr = child
        .child_mut()
        .stderr
        .take()
        .ok_or_else(|| io::Error::other("Pi bridge stderr was unavailable"))?;
    let (stdout_tx, stdout_rx) = mpsc::channel();
    let (stderr_tx, stderr_rx) = mpsc::channel();
    std::thread::Builder::new()
        .name("loxa-pi-bridge-stdout".into())
        .spawn(move || {
            let _ = stdout_tx.send(read_bounded(stdout, MAX_BRIDGE_BYTES));
        })?;
    std::thread::Builder::new()
        .name("loxa-pi-bridge-stderr".into())
        .spawn(move || {
            let _ = stderr_tx.send(read_bounded(stderr, MAX_BRIDGE_BYTES));
        })?;

    let status = poll_until_deadline(process_deadline, || child.child_mut().try_wait())?;
    let remaining = deadline.saturating_duration_since(Instant::now());
    let stdout = stdout_rx
        .recv_timeout(remaining)
        .map_err(|_| io::Error::other("Pi bridge stdout did not terminate"))??;
    let remaining = deadline.saturating_duration_since(Instant::now());
    let stderr = stderr_rx
        .recv_timeout(remaining)
        .map_err(|_| io::Error::other("Pi bridge stderr did not terminate"))??;
    #[cfg(unix)]
    kill_owned_bridge_group(child.child_mut());
    child.disarm();
    Ok(BridgeOutput {
        status,
        stdout,
        stderr,
    })
}

struct BridgeChildGuard {
    child: Option<std::process::Child>,
    cancellation_grace: Duration,
}

impl BridgeChildGuard {
    fn new(child: std::process::Child, cancellation_grace: Duration) -> Self {
        Self {
            child: Some(child),
            cancellation_grace,
        }
    }

    fn child_mut(&mut self) -> &mut std::process::Child {
        self.child
            .as_mut()
            .expect("bridge child guard must remain armed")
    }

    fn disarm(&mut self) {
        self.child.take();
    }
}

impl Drop for BridgeChildGuard {
    fn drop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        let cancellation_requested = child
            .stdin
            .take()
            .is_some_and(|mut control| control.write_all(&[1]).is_ok());
        let cancellation_grace = if cancellation_requested {
            self.cancellation_grace
        } else {
            Duration::ZERO
        };
        let (sender, receiver) = mpsc::sync_channel::<(std::process::Child, Duration)>(1);
        match std::thread::Builder::new()
            .name("loxa-pi-bridge-reaper".into())
            .spawn(move || {
                if let Ok((child, cancellation_grace)) = receiver.recv() {
                    reap_bridge_child(child, cancellation_grace);
                }
            }) {
            Ok(_) => {
                if let Err(error) = sender.send((child, cancellation_grace)) {
                    let (child, cancellation_grace) = error.0;
                    reap_bridge_child(child, cancellation_grace);
                }
            }
            Err(_) => {
                reap_bridge_child(child, cancellation_grace);
            }
        }
    }
}

fn reap_bridge_child(mut child: std::process::Child, cancellation_grace: Duration) {
    let deadline = Instant::now() + cancellation_grace;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => {
                #[cfg(unix)]
                kill_owned_bridge_group(&child);
                return;
            }
            Ok(None) if Instant::now() < deadline => {
                std::thread::sleep(
                    Duration::from_millis(10)
                        .min(deadline.saturating_duration_since(Instant::now())),
                );
            }
            Ok(None) | Err(_) => break,
        }
    }
    force_kill_bridge_tree(&mut child);
    let _ = child.wait();
}

#[cfg(unix)]
fn force_kill_bridge_tree(child: &mut std::process::Child) {
    kill_owned_bridge_group(child);
    let _ = child.kill();
}

#[cfg(unix)]
fn kill_owned_bridge_group(child: &std::process::Child) {
    // SAFETY: getpgrp has no preconditions. A checked child PID above one that is
    // different from Loxa's group can only address the child-owned bridge group.
    let loxa_group = unsafe { libc::getpgrp() };
    let Some(pid) = checked_owned_bridge_group(child.id(), loxa_group) else {
        return;
    };
    // SAFETY: live bridge children are launched with process_group(0), and the
    // checks above prevent signaling PID 1 or Loxa's own process group.
    unsafe {
        libc::kill(-pid, libc::SIGKILL);
    }
}

#[cfg(unix)]
fn checked_owned_bridge_group(child_pid: u32, loxa_group: i32) -> Option<i32> {
    let pid = i32::try_from(child_pid).ok()?;
    (pid > 1 && pid != loxa_group).then_some(pid)
}

#[cfg(windows)]
fn force_kill_bridge_tree(child: &mut std::process::Child) {
    if let Some(system_root) = std::env::var_os("SystemRoot") {
        let taskkill = PathBuf::from(system_root)
            .join("System32")
            .join("taskkill.exe");
        if let Ok(mut killer) = Command::new(taskkill)
            .args(["/PID", &child.id().to_string(), "/T", "/F"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
        {
            let deadline = Instant::now() + Duration::from_millis(250);
            while Instant::now() < deadline {
                if killer.try_wait().is_ok_and(|status| status.is_some()) {
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            let _ = killer.kill();
            let _ = killer.wait();
        }
    }
    let _ = child.kill();
}

fn poll_until_deadline<T>(
    deadline: Instant,
    mut poll: impl FnMut() -> io::Result<Option<T>>,
) -> io::Result<T> {
    loop {
        if let Some(value) = poll()? {
            return Ok(value);
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "Pi bridge exceeded its outer deadline",
            ));
        }
        std::thread::sleep(
            Duration::from_millis(10).min(deadline.saturating_duration_since(Instant::now())),
        );
    }
}

fn read_bounded(mut reader: impl Read, maximum: usize) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader
        .by_ref()
        .take(maximum as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > maximum {
        return Err(io::Error::other("Pi bridge output exceeded its size limit"));
    }
    Ok(bytes)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BridgeResult {
    schema_version: u32,
    tool_trace: Vec<ToolTraceRecord>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ToolTraceRecord {
    tool: String,
    status: String,
    #[serde(default)]
    stage: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SanitizedEvidence {
    schema_version: u32,
    pub(crate) phase: AcceptancePhase,
    pub(crate) provider_config_sha256: String,
    models_before: bool,
    ready_before: bool,
    tool_order: bool,
    exact_workspace: bool,
    verification: bool,
    models_after: bool,
    ready_after: bool,
}

#[derive(Serialize)]
struct ModelsConfig<'a> {
    providers: Providers<'a>,
}

#[derive(Serialize)]
struct Providers<'a> {
    loxa: Provider<'a>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Provider<'a> {
    base_url: &'a str,
    api: &'static str,
    api_key: &'static str,
    models: [Model; 1],
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Model {
    id: &'static str,
    name: &'static str,
    reasoning: bool,
    input: [&'static str; 1],
    context_window: u16,
    max_tokens: u16,
    compat: Compat,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Compat {
    max_tokens_field: &'static str,
}

pub(crate) fn run_live(request: PiAcceptanceRequest) -> io::Result<SanitizedEvidence> {
    let execution_root = std::env::current_dir()?;
    let mut runtime = LiveRuntime::new()?;
    run_with_runtime(request, &execution_root, &mut runtime)
}

pub(crate) fn write_evidence_json(
    evidence: &SanitizedEvidence,
    output: &mut impl Write,
) -> io::Result<()> {
    serde_json::to_writer(&mut *output, evidence).map_err(io::Error::other)?;
    output.write_all(b"\n")
}

pub(crate) fn run_with_runtime(
    request: PiAcceptanceRequest,
    repository_root: &Path,
    runtime: &mut impl AcceptanceRuntime,
) -> io::Result<SanitizedEvidence> {
    let endpoint = validate_request(&request, repository_root)?;
    let evidence_directory = repository_root.join(&request.evidence_dir);
    ensure_safe_evidence_directory(repository_root, &request.evidence_dir)?;

    let temporary = Builder::new().prefix("loxa-pi-acceptance-").tempdir()?;
    #[cfg(target_os = "macos")]
    let temporary_root = fs::canonicalize(temporary.path())?;
    #[cfg(not(target_os = "macos"))]
    let temporary_root = temporary.path().to_path_buf();
    let home = temporary_root.join("home");
    let config_directory = temporary_root.join("pi-config");
    let workspace = temporary_root.join("workspace");
    let child_temp = temporary_root.join("tmp");
    for directory in [&home, &config_directory, &workspace, &child_temp] {
        fs::create_dir(directory)?;
    }

    let seed = temporary_root.join("seed");
    fs::create_dir(&seed)?;
    materialize_fixture(&seed)?;
    copy_tree(&seed, &workspace)?;
    let authoritative_workspace = snapshot_tree(&workspace)?;
    let prompt = PI_ACCEPTANCE_PROMPT.to_owned();
    if prompt.is_empty() || prompt.len() > 32 * 1024 || prompt.contains('\0') {
        return Err(invalid("Pi acceptance prompt is invalid"));
    }

    let config_bytes = models_config_bytes(&endpoint.base_url, request.max_tokens)?;
    let provider_config_sha256 = sha256(&config_bytes);
    if let Some(expected) = request.expected_config_sha256.as_deref() {
        if provider_config_sha256 != expected {
            return Err(invalid("provider config digest changed"));
        }
    }
    let config_path = config_directory.join("models.json");
    write_private_file(&config_path, &config_bytes)?;

    validate_gateway(&endpoint, runtime)?;
    let extension = temporary_root.join("acceptance-gate.mjs");
    write_private_file(&extension, PI_ACCEPTANCE_EXTENSION)?;
    let invocation = BridgeInvocation {
        pi_entrypoint: request.pi_entrypoint,
        extension,
        prompt,
        workspace: workspace.clone(),
        environment: isolated_environment(&home, &config_directory, &child_temp),
    };
    let bridge_bytes = runtime.run_bridge(&invocation)?;
    validate_bridge_result(&bridge_bytes)?;
    validate_exact_workspace_against(&authoritative_workspace, &workspace)?;
    let verified_workspace = snapshot_tree(&workspace)?;
    run_independent_verifier(&workspace, PI_INDEPENDENT_VERIFIER, &invocation.environment)?;
    validate_exact_workspace_against(&authoritative_workspace, &workspace)?;
    if snapshot_tree(&workspace)? != verified_workspace {
        return Err(invalid("workspace changed during independent verification"));
    }
    validate_gateway(&endpoint, runtime)?;

    let evidence = SanitizedEvidence {
        schema_version: 1,
        phase: request.phase,
        provider_config_sha256,
        models_before: true,
        ready_before: true,
        tool_order: true,
        exact_workspace: true,
        verification: true,
        models_after: true,
        ready_after: true,
    };
    write_evidence_noclobber(&evidence_directory, &evidence)?;
    Ok(evidence)
}

struct ValidatedEndpoint {
    base_url: String,
    models_url: String,
    status_url: String,
}

fn validate_request(
    request: &PiAcceptanceRequest,
    repository_root: &Path,
) -> io::Result<ValidatedEndpoint> {
    if request.max_tokens == 0 || request.max_tokens >= 8192 {
        return Err(invalid("max tokens must be an integer between 1 and 8191"));
    }
    if !request.pi_entrypoint.is_absolute() || !request.pi_entrypoint.is_file() {
        return Err(invalid(
            "Pi CLI entrypoint must be an absolute regular file",
        ));
    }
    if request
        .expected_config_sha256
        .as_deref()
        .is_some_and(|digest| !valid_digest(digest))
    {
        return Err(invalid(
            "expected provider config digest must be a lowercase SHA-256 digest",
        ));
    }
    if request.phase == AcceptancePhase::PostRecovery && request.expected_config_sha256.is_none() {
        return Err(invalid(
            "post-recovery requires the expected provider config digest",
        ));
    }
    validate_evidence_relative(&request.evidence_dir)?;
    if !repository_root.is_absolute() {
        return Err(invalid("repository root must be absolute"));
    }

    let parsed = Url::parse(&request.base_url)
        .map_err(|_| invalid("base URL must be a valid absolute URL"))?;
    if !matches!(parsed.scheme(), "http" | "https")
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || parsed.path() != "/v1"
        || parsed.host_str().is_none()
        || parsed.host_str() == Some("0.0.0.0")
    {
        return Err(invalid(
            "base URL must be credential-free and end at exact /v1",
        ));
    }
    let host = parsed.host_str().unwrap_or_default();
    match request.phase {
        AcceptancePhase::MacLocal if host != "127.0.0.1" => {
            return Err(invalid("mac-local requires the IPv4 loopback endpoint"));
        }
        AcceptancePhase::WindowsTailnet
            if parsed.scheme() != "https"
                || is_loopback_host(host)
                || !valid_tailnet_hostname(host) =>
        {
            return Err(invalid(
                "windows-tailnet requires a non-loopback HTTPS tailnet endpoint",
            ));
        }
        _ => {}
    }

    let mut models = parsed.clone();
    models.set_path("/v1/models");
    let mut status = parsed.clone();
    status.set_path("/loxa/status");
    Ok(ValidatedEndpoint {
        base_url: parsed.to_string(),
        models_url: models.to_string(),
        status_url: status.to_string(),
    })
}

fn valid_tailnet_hostname(host: &str) -> bool {
    let Some(prefix) = host.strip_suffix(".ts.net") else {
        return false;
    };
    !prefix.is_empty()
        && prefix.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        })
}

fn is_loopback_host(host: &str) -> bool {
    matches!(host, "127.0.0.1" | "localhost" | "::1")
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_gateway(
    endpoint: &ValidatedEndpoint,
    runtime: &mut impl AcceptanceRuntime,
) -> io::Result<()> {
    let models: serde_json::Value =
        serde_json::from_slice(&runtime.gateway_json(&endpoint.models_url)?)
            .map_err(|_| io::Error::other("gateway models response was invalid"))?;
    let data = models
        .get("data")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| io::Error::other("models response does not contain stable loxa model"))?;
    if data.len() > 10_000
        || !data
            .iter()
            .any(|model| model.get("id").and_then(serde_json::Value::as_str) == Some("loxa"))
    {
        return Err(io::Error::other(
            "models response does not contain stable loxa model",
        ));
    }

    let status: serde_json::Value =
        serde_json::from_slice(&runtime.gateway_json(&endpoint.status_url)?)
            .map_err(|_| io::Error::other("gateway status response was invalid"))?;
    let version = status
        .pointer("/engine/version")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let mut version_lines = version.split('\n');
    let identity_matches = version_lines.next() == Some(QUALIFIED_LLAMA_CPP_RUNTIME_IDENTITY)
        && version_lines
            .next()
            .is_none_or(|line| !line.contains(['\r', '\n']))
        && version_lines.next().is_none();
    if status.get("health").and_then(serde_json::Value::as_str) != Some("ready")
        || status.get("model").and_then(serde_json::Value::as_str) != Some("loxa")
        || status
            .pointer("/engine/name")
            .and_then(serde_json::Value::as_str)
            != Some("llama-cpp")
        || !identity_matches
    {
        return Err(io::Error::other(
            "status response is not ready for the qualified stable loxa model",
        ));
    }
    Ok(())
}

fn models_config_bytes(base_url: &str, max_tokens: u16) -> io::Result<Vec<u8>> {
    let config = ModelsConfig {
        providers: Providers {
            loxa: Provider {
                base_url,
                api: "openai-completions",
                api_key: "loxa-dummy-key",
                models: [Model {
                    id: "loxa",
                    name: "Loxa",
                    reasoning: false,
                    input: ["text"],
                    context_window: 8192,
                    max_tokens,
                    compat: Compat {
                        max_tokens_field: "max_tokens",
                    },
                }],
            },
        },
    };
    let mut bytes = serde_json::to_vec_pretty(&config).map_err(io::Error::other)?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn isolated_environment(
    home: &Path,
    config: &Path,
    child_temp: &Path,
) -> BTreeMap<String, OsString> {
    #[cfg(target_os = "windows")]
    let platform = EnvironmentPlatform::Windows;
    #[cfg(not(target_os = "windows"))]
    let platform = EnvironmentPlatform::Mac;
    let source = std::env::vars_os()
        .filter_map(|(key, value)| Some((key.to_str()?.to_string(), value)))
        .collect();
    isolated_environment_for(
        platform,
        home.as_os_str(),
        config.as_os_str(),
        child_temp.as_os_str(),
        &source,
    )
}

#[derive(Clone, Copy)]
enum EnvironmentPlatform {
    Mac,
    #[cfg(any(test, target_os = "windows"))]
    Windows,
}

fn isolated_environment_for(
    platform: EnvironmentPlatform,
    home: &std::ffi::OsStr,
    config: &std::ffi::OsStr,
    child_temp: &std::ffi::OsStr,
    source: &BTreeMap<String, OsString>,
) -> BTreeMap<String, OsString> {
    let mut environment = BTreeMap::new();
    let inherited = match platform {
        EnvironmentPlatform::Mac => &["PATH", "LANG", "LC_ALL", "LC_CTYPE"][..],
        #[cfg(any(test, target_os = "windows"))]
        EnvironmentPlatform::Windows => &[
            "PATH",
            "PATHEXT",
            "SYSTEMROOT",
            "WINDIR",
            "COMSPEC",
            "LANG",
            "LC_ALL",
            "LC_CTYPE",
        ][..],
    };
    for key in inherited {
        let value = match platform {
            EnvironmentPlatform::Mac => source.get(*key),
            #[cfg(any(test, target_os = "windows"))]
            EnvironmentPlatform::Windows => source
                .iter()
                .find(|(candidate, _)| candidate.eq_ignore_ascii_case(key))
                .map(|(_, value)| value),
        };
        if let Some(value) = value {
            environment.insert((*key).to_string(), value.clone());
        }
    }
    match platform {
        EnvironmentPlatform::Mac => {
            environment.insert("HOME".into(), home.to_os_string());
            environment.insert("XDG_CONFIG_HOME".into(), append_os(home, "/.config"));
            environment.insert("XDG_CACHE_HOME".into(), append_os(home, "/.cache"));
            environment.insert("XDG_DATA_HOME".into(), append_os(home, "/.local/share"));
            environment.insert("TMPDIR".into(), child_temp.to_os_string());
        }
        #[cfg(any(test, target_os = "windows"))]
        EnvironmentPlatform::Windows => {
            let home_text = home.to_string_lossy();
            let (drive, home_path) = if home_text.len() >= 3
                && home_text.as_bytes()[1] == b':'
                && matches!(home_text.as_bytes()[2], b'\\' | b'/')
            {
                (&home_text[..2], &home_text[2..])
            } else {
                ("", "")
            };
            environment.insert("HOME".into(), home.to_os_string());
            environment.insert("USERPROFILE".into(), home.to_os_string());
            environment.insert("HOMEDRIVE".into(), OsString::from(drive));
            environment.insert("HOMEPATH".into(), OsString::from(home_path));
            environment.insert("APPDATA".into(), append_os(home, r"\AppData\Roaming"));
            environment.insert("LOCALAPPDATA".into(), append_os(home, r"\AppData\Local"));
            environment.insert("XDG_CONFIG_HOME".into(), append_os(home, r"\.config"));
            environment.insert("XDG_CACHE_HOME".into(), append_os(home, r"\.cache"));
            environment.insert("XDG_DATA_HOME".into(), append_os(home, r"\.local\share"));
            environment.insert("TEMP".into(), child_temp.to_os_string());
            environment.insert("TMP".into(), child_temp.to_os_string());
        }
    }
    environment.insert("PI_CODING_AGENT_DIR".into(), config.to_os_string());
    for (key, value) in [
        ("PI_OFFLINE", "1"),
        ("PI_TELEMETRY", "0"),
        ("PI_SKIP_VERSION_CHECK", "1"),
    ] {
        environment.insert(key.to_string(), OsString::from(value));
    }
    environment
}

fn append_os(base: &std::ffi::OsStr, suffix: &str) -> OsString {
    let mut value = base.to_os_string();
    value.push(suffix);
    value
}

fn materialize_fixture(seed: &Path) -> io::Result<()> {
    let source_directory = seed.join("src");
    let test_directory = seed.join("test");
    fs::create_dir(&source_directory)?;
    fs::create_dir(&test_directory)?;
    for (path, bytes) in [
        (seed.join("package.json"), PI_FIXTURE_PACKAGE),
        (source_directory.join("merge-ranges.mjs"), PI_FIXTURE_SOURCE),
        (test_directory.join("verify.mjs"), PI_FIXTURE_VERIFIER),
    ] {
        write_private_file(&path, bytes)?;
    }
    Ok(())
}

fn copy_tree(source: &Path, destination: &Path) -> io::Result<()> {
    let metadata = fs::symlink_metadata(source)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(invalid("Pi seed root must be a real directory"));
    }
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let metadata = fs::symlink_metadata(&source_path)?;
        if metadata.file_type().is_symlink() {
            return Err(invalid("Pi seed cannot contain symbolic links"));
        }
        if metadata.is_dir() {
            fs::create_dir(&destination_path)?;
            copy_tree(&source_path, &destination_path)?;
        } else if metadata.is_file() {
            fs::copy(&source_path, &destination_path)?;
        } else {
            return Err(invalid("Pi seed contains an unsupported file type"));
        }
    }
    Ok(())
}

#[derive(PartialEq, Eq)]
struct TreeEntry {
    kind: &'static str,
    bytes: Vec<u8>,
    #[cfg(unix)]
    mode: u32,
}

fn snapshot_tree(root: &Path) -> io::Result<BTreeMap<String, TreeEntry>> {
    fn walk(
        root: &Path,
        directory: &Path,
        output: &mut BTreeMap<String, TreeEntry>,
        folded: &mut BTreeSet<String>,
    ) -> io::Result<()> {
        let mut entries = fs::read_dir(directory)?.collect::<Result<Vec<_>, _>>()?;
        entries.sort_by_key(std::fs::DirEntry::file_name);
        for entry in entries {
            let path = entry.path();
            let relative = path
                .strip_prefix(root)
                .map_err(io::Error::other)?
                .to_string_lossy()
                .replace('\\', "/");
            if !folded.insert(relative.to_lowercase()) {
                return Err(invalid("workspace contains a case-only path collision"));
            }
            let metadata = fs::symlink_metadata(&path)?;
            if metadata.file_type().is_symlink() {
                return Err(invalid("workspace contains a symbolic link"));
            }
            #[cfg(unix)]
            let mode = {
                use std::os::unix::fs::PermissionsExt;
                metadata.permissions().mode() & 0o777
            };
            if metadata.is_dir() {
                output.insert(
                    relative,
                    TreeEntry {
                        kind: "directory",
                        bytes: Vec::new(),
                        #[cfg(unix)]
                        mode,
                    },
                );
                walk(root, &path, output, folded)?;
            } else if metadata.is_file() {
                #[cfg(unix)]
                if metadata.nlink() != 1 {
                    return Err(invalid("workspace contains a hard-linked file"));
                }
                output.insert(
                    relative,
                    TreeEntry {
                        kind: "file",
                        bytes: fs::read(&path)?,
                        #[cfg(unix)]
                        mode,
                    },
                );
            } else {
                return Err(invalid("workspace contains an unsupported file type"));
            }
        }
        Ok(())
    }
    let mut output = BTreeMap::new();
    walk(root, root, &mut output, &mut BTreeSet::new())?;
    Ok(output)
}

#[cfg(test)]
fn validate_exact_workspace(seed: &Path, workspace: &Path) -> io::Result<()> {
    let before = snapshot_tree(seed)?;
    validate_exact_workspace_against(&before, workspace)
}

fn validate_exact_workspace_against(
    before: &BTreeMap<String, TreeEntry>,
    workspace: &Path,
) -> io::Result<()> {
    let after = snapshot_tree(workspace)?;
    if before.keys().ne(after.keys()) {
        return Err(invalid("workspace has extra, deleted, or renamed paths"));
    }
    let mut changes = 0;
    for (path, original) in before {
        let current = after
            .get(path)
            .ok_or_else(|| invalid("workspace path disappeared"))?;
        if original.kind != current.kind {
            return Err(invalid("workspace contains a file type change"));
        }
        #[cfg(unix)]
        if original.mode != current.mode {
            return Err(invalid("workspace contains a mode change"));
        }
        if original.bytes != current.bytes {
            changes += 1;
            if path != "src/merge-ranges.mjs" {
                return Err(invalid("workspace contains an unrelated byte change"));
            }
        }
    }
    if changes != 1 {
        return Err(invalid(
            "workspace must contain exactly one expected byte change",
        ));
    }
    Ok(())
}

fn run_independent_verifier(
    workspace: &Path,
    verifier_source: &str,
    environment: &BTreeMap<String, OsString>,
) -> io::Result<()> {
    let mut entropy = [0_u8; 32];
    getrandom::fill(&mut entropy)
        .map_err(|_| io::Error::other("independent verifier challenge was unavailable"))?;
    let challenge = sha256(&entropy);
    let mut command = Command::new("node");
    command
        .args(["--input-type=module", "--eval", verifier_source])
        .current_dir(workspace)
        .env_clear()
        .envs(environment)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(unix)]
    command.process_group(0);
    let mut child = command
        .spawn()
        .map_err(|_| io::Error::other("independent verifier failed to start"))?;
    let challenge_result = child
        .stdin
        .as_mut()
        .ok_or_else(|| io::Error::other("independent verifier stdin was unavailable"))
        .and_then(|stdin| {
            stdin.write_all(challenge.as_bytes())?;
            stdin.write_all(b"\n")
        });
    child.stdin.take();
    if challenge_result.is_err() {
        force_kill_bridge_tree(&mut child);
        let _ = child.wait();
        return Err(io::Error::other(
            "independent verifier challenge delivery failed",
        ));
    }
    let output = wait_for_bridge(child, INDEPENDENT_VERIFIER_TIMEOUT).map_err(|error| {
        io::Error::new(
            error.kind(),
            "independent verifier did not terminate safely",
        )
    })?;
    let expected = format!("LOXA_PI_ACCEPTANCE_PASS {challenge}\n");
    // The challenged verifier writes bytes directly, so the exact LF record is
    // identical on macOS and Windows and needs no lossy text normalization.
    if !output.status.success() || output.stdout != expected.as_bytes() || !output.stderr.is_empty()
    {
        return Err(io::Error::other(
            "independent verifier did not produce the exact PASS result",
        ));
    }
    Ok(())
}

fn validate_bridge_result(bytes: &[u8]) -> io::Result<()> {
    if bytes.len() > MAX_BRIDGE_BYTES {
        return Err(invalid("Pi bridge result exceeded its size limit"));
    }
    let result: BridgeResult =
        serde_json::from_slice(bytes).map_err(|_| invalid("Pi bridge result was invalid"))?;
    if result.schema_version != 1 || result.tool_trace.len() != 5 {
        return Err(invalid(
            "Pi bridge result is missing the exact repair tool loop",
        ));
    }
    let expected = [
        ("read", "success", None),
        ("read", "success", None),
        ("bash", "expected-failure", Some("failing-verification")),
        ("write", "success", None),
        ("bash", "success", Some("verification")),
    ];
    if result
        .tool_trace
        .iter()
        .zip(expected)
        .any(|(record, (tool, status, stage))| {
            record.tool != tool || record.status != status || record.stage.as_deref() != stage
        })
    {
        return Err(invalid(
            "Pi bridge result is missing the exact repair tool loop",
        ));
    }
    Ok(())
}

fn validate_evidence_relative(relative: &Path) -> io::Result<()> {
    let components = relative.components().collect::<Vec<_>>();
    if components.len() < 2
        || components[0] != Component::Normal("target".as_ref())
        || components[1] != Component::Normal("pi-acceptance".as_ref())
        || components
            .iter()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(invalid(
            "evidence directory must be under target/pi-acceptance",
        ));
    }
    Ok(())
}

fn ensure_safe_evidence_directory(repository_root: &Path, relative: &Path) -> io::Result<()> {
    let mut current = repository_root.to_path_buf();
    for component in relative.components() {
        let Component::Normal(component) = component else {
            return Err(invalid(
                "evidence directory must be under target/pi-acceptance",
            ));
        };
        current.push(component);
        match fs::create_dir(&current) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
        let metadata = fs::symlink_metadata(&current)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(invalid("evidence directory is unsafe"));
        }
    }
    Ok(())
}

fn write_evidence_noclobber(directory: &Path, evidence: &SanitizedEvidence) -> io::Result<()> {
    let destination = directory.join("evidence.json");
    let mut temporary = NamedTempFile::new_in(directory)?;
    let mut bytes = serde_json::to_vec(evidence).map_err(io::Error::other)?;
    bytes.push(b'\n');
    temporary.write_all(&bytes)?;
    temporary.as_file().sync_all()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        temporary
            .as_file()
            .set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    temporary.persist_noclobber(destination).map_err(|error| {
        if error.error.kind() == io::ErrorKind::AlreadyExists {
            invalid("evidence artifact already exists")
        } else {
            error.error
        }
    })?;
    Ok(())
}

fn write_private_file(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

fn sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        write!(encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::fs;
    use std::path::{Path, PathBuf};

    struct FakeRuntime {
        gateway: VecDeque<Vec<u8>>,
        gateway_urls: Vec<String>,
        bridge_calls: usize,
        temporary_root: Option<PathBuf>,
        temporary_root_is_canonical: bool,
        config_bytes: Option<Vec<u8>>,
        source_replacement: Vec<u8>,
        precreate_independent_verifier: bool,
        mutate_seed_and_workspace: bool,
    }

    impl FakeRuntime {
        fn ready() -> Self {
            let models = br#"{"data":[{"id":"loxa"}]}"#.to_vec();
            let status = format!(
                r#"{{"health":"ready","model":"loxa","engine":{{"name":"llama-cpp","version":"{}\nbuilt with AppleClang"}}}}"#,
                QUALIFIED_LLAMA_CPP_RUNTIME_IDENTITY
            )
            .into_bytes();
            Self {
                gateway: VecDeque::from([models.clone(), status.clone(), models, status]),
                gateway_urls: Vec::new(),
                bridge_calls: 0,
                temporary_root: None,
                temporary_root_is_canonical: false,
                config_bytes: None,
                source_replacement: PI_EXPECTED_SOURCE.to_vec(),
                precreate_independent_verifier: false,
                mutate_seed_and_workspace: false,
            }
        }
    }

    impl AcceptanceRuntime for FakeRuntime {
        fn gateway_json(&mut self, url: &str) -> std::io::Result<Vec<u8>> {
            self.gateway_urls.push(url.to_string());
            self.gateway.pop_front().ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "no gateway response")
            })
        }

        fn run_bridge(&mut self, invocation: &BridgeInvocation) -> std::io::Result<Vec<u8>> {
            self.bridge_calls += 1;
            self.temporary_root = invocation.workspace.parent().map(Path::to_path_buf);
            self.temporary_root_is_canonical = if cfg!(target_os = "macos") {
                fs::canonicalize(
                    invocation
                        .workspace
                        .parent()
                        .expect("workspace has temporary root"),
                )? == invocation
                    .workspace
                    .parent()
                    .expect("workspace has temporary root")
            } else {
                true
            };
            let config_directory = PathBuf::from(
                invocation
                    .environment
                    .get("PI_CODING_AGENT_DIR")
                    .expect("config directory"),
            );
            self.config_bytes = Some(fs::read(config_directory.join("models.json"))?);
            assert_eq!(
                fs::read(invocation.workspace.join("src/merge-ranges.mjs"))?,
                PI_FIXTURE_SOURCE
            );
            assert_eq!(
                fs::read(invocation.workspace.join("test/verify.mjs"))?,
                PI_FIXTURE_VERIFIER
            );
            assert_eq!(fs::read(&invocation.extension)?, PI_ACCEPTANCE_EXTENSION);
            fs::write(
                invocation.workspace.join("src/merge-ranges.mjs"),
                &self.source_replacement,
            )?;
            if self.precreate_independent_verifier {
                fs::write(
                    invocation
                        .workspace
                        .parent()
                        .expect("temporary root")
                        .join("independent-verify.mjs"),
                    b"let challenge = \"\";\nprocess.stdin.setEncoding(\"utf8\");\nfor await (const chunk of process.stdin) challenge += chunk;\nprocess.stdout.write(`LOXA_PI_ACCEPTANCE_PASS ${challenge.trim()}\\n`);\n",
                )?;
            }
            if self.mutate_seed_and_workspace {
                let mutated = b"// bridge-mutated verifier\n";
                fs::write(
                    invocation
                        .workspace
                        .parent()
                        .expect("temporary root")
                        .join("seed/test/verify.mjs"),
                    mutated,
                )?;
                fs::write(invocation.workspace.join("test/verify.mjs"), mutated)?;
            }
            Ok(br#"{"schemaVersion":1,"toolTrace":[{"tool":"read","status":"success"},{"tool":"read","status":"success"},{"tool":"bash","stage":"failing-verification","status":"expected-failure"},{"tool":"write","status":"success"},{"tool":"bash","stage":"verification","status":"success"}]}"#.to_vec())
        }
    }

    fn request(root: &Path, phase: AcceptancePhase) -> PiAcceptanceRequest {
        PiAcceptanceRequest {
            phase,
            base_url: "http://127.0.0.1:11435/v1".into(),
            pi_entrypoint: root.join("pi"),
            max_tokens: 4096,
            expected_config_sha256: None,
            evidence_dir: PathBuf::from("target/pi-acceptance/test"),
        }
    }

    #[test]
    fn live_runtime_materializes_the_bridge_without_a_source_checkout() {
        let runtime = LiveRuntime::new().unwrap();

        assert!(
            runtime.bridge.is_file(),
            "the runnable bridge must be materialized from the binary"
        );
    }

    #[test]
    fn acceptance_materializes_its_fixture_without_a_source_checkout() {
        let execution_root = tempfile::tempdir().unwrap();
        fs::write(execution_root.path().join("pi"), b"fake").unwrap();

        let evidence = run_with_runtime(
            request(execution_root.path(), AcceptancePhase::MacLocal),
            execution_root.path(),
            &mut FakeRuntime::ready(),
        )
        .expect("embedded acceptance fixture must run without repository files");

        assert!(evidence.exact_workspace);
        assert!(execution_root
            .path()
            .join("target/pi-acceptance/test/evidence.json")
            .is_file());
    }

    #[test]
    fn independent_verifier_rejects_an_early_exit_and_publishes_no_evidence() {
        let execution_root = tempfile::tempdir().unwrap();
        fs::write(execution_root.path().join("pi"), b"fake").unwrap();
        let mut runtime = FakeRuntime::ready();
        runtime.source_replacement =
            b"process.exit(0);\nexport function mergeRanges() { return []; }\n".to_vec();

        let error = run_with_runtime(
            request(execution_root.path(), AcceptancePhase::MacLocal),
            execution_root.path(),
            &mut runtime,
        )
        .unwrap_err();

        assert!(error.to_string().contains("independent verifier"));
        assert!(!execution_root
            .path()
            .join("target/pi-acceptance/test/evidence.json")
            .exists());
    }

    #[test]
    fn independent_verifier_rejects_forged_fixed_success_output() {
        let execution_root = tempfile::tempdir().unwrap();
        fs::write(execution_root.path().join("pi"), b"fake").unwrap();
        let mut runtime = FakeRuntime::ready();
        runtime.source_replacement = b"process.stdout.write(\"PASS 4 checks\\n\"); process.exit(0);\nexport function mergeRanges() { return []; }\n".to_vec();

        let result = run_with_runtime(
            request(execution_root.path(), AcceptancePhase::MacLocal),
            execution_root.path(),
            &mut runtime,
        );

        assert!(result.is_err());
        assert!(!execution_root
            .path()
            .join("target/pi-acceptance/test/evidence.json")
            .exists());
    }

    #[test]
    fn bridge_created_sibling_verifier_has_no_authority() {
        let execution_root = tempfile::tempdir().unwrap();
        fs::write(execution_root.path().join("pi"), b"fake").unwrap();
        let mut runtime = FakeRuntime::ready();
        runtime.precreate_independent_verifier = true;
        runtime.source_replacement = b"export function mergeRanges() { return []; }\n".to_vec();

        let result = run_with_runtime(
            request(execution_root.path(), AcceptancePhase::MacLocal),
            execution_root.path(),
            &mut runtime,
        );

        assert!(result.is_err());
        assert!(!execution_root
            .path()
            .join("target/pi-acceptance/test/evidence.json")
            .exists());
    }

    #[test]
    fn bridge_cannot_redefine_the_seed_baseline_with_matching_workspace_bytes() {
        let execution_root = tempfile::tempdir().unwrap();
        fs::write(execution_root.path().join("pi"), b"fake").unwrap();
        let mut runtime = FakeRuntime::ready();
        runtime.mutate_seed_and_workspace = true;

        let result = run_with_runtime(
            request(execution_root.path(), AcceptancePhase::MacLocal),
            execution_root.path(),
            &mut runtime,
        );

        assert!(result.is_err());
        assert!(!execution_root
            .path()
            .join("target/pi-acceptance/test/evidence.json")
            .exists());
    }

    #[test]
    fn import_time_workspace_mutation_publishes_no_evidence() {
        let execution_root = tempfile::tempdir().unwrap();
        fs::write(execution_root.path().join("pi"), b"fake").unwrap();
        let mut runtime = FakeRuntime::ready();
        runtime.source_replacement =
            b"import { writeFileSync } from \"node:fs\";\nwriteFileSync(\"extra.txt\", \"extra\\n\");\n"
                .to_vec();
        runtime
            .source_replacement
            .extend_from_slice(PI_EXPECTED_SOURCE);

        let result = run_with_runtime(
            request(execution_root.path(), AcceptancePhase::MacLocal),
            execution_root.path(),
            &mut runtime,
        );

        assert!(result.is_err());
        assert!(!execution_root
            .path()
            .join("target/pi-acceptance/test/evidence.json")
            .exists());
    }

    #[test]
    fn import_time_source_self_modification_publishes_no_evidence() {
        let execution_root = tempfile::tempdir().unwrap();
        fs::write(execution_root.path().join("pi"), b"fake").unwrap();
        let mut runtime = FakeRuntime::ready();
        runtime.source_replacement = br#"import { writeFileSync } from "node:fs";
writeFileSync("src/merge-ranges.mjs", "export function mergeRanges() { return []; }\n");
"#
        .to_vec();
        runtime
            .source_replacement
            .extend_from_slice(PI_EXPECTED_SOURCE);

        let result = run_with_runtime(
            request(execution_root.path(), AcceptancePhase::MacLocal),
            execution_root.path(),
            &mut runtime,
        );

        assert!(result.is_err());
        assert!(!execution_root
            .path()
            .join("target/pi-acceptance/test/evidence.json")
            .exists());
    }

    #[test]
    fn independent_verifier_requires_exact_status_stdout_and_stderr() {
        let environment = std::env::vars_os()
            .filter_map(|(key, value)| Some((key.to_str()?.to_string(), value)))
            .collect::<BTreeMap<_, _>>();
        let read_challenge = "let challenge = \"\";\nprocess.stdin.setEncoding(\"utf8\");\nfor await (const chunk of process.stdin) challenge += chunk;\nchallenge = challenge.trim();\n";
        for (body, succeeds) in [
            (
                "process.stdout.write(`LOXA_PI_ACCEPTANCE_PASS ${challenge}\\n`);\n",
                true,
            ),
            (
                "process.stdout.write(`LOXA_PI_ACCEPTANCE_PASS ${challenge}\\nextra\\n`);\n",
                false,
            ),
            (
                "process.stdout.write(`LOXA_PI_ACCEPTANCE_PASS ${challenge}\\n`); console.error(\"extra\");\n",
                false,
            ),
            (
                "process.stdout.write(`LOXA_PI_ACCEPTANCE_PASS ${challenge}\\n`); process.exitCode = 1;\n",
                false,
            ),
        ] {
            let root = tempfile::tempdir().unwrap();
            let workspace = root.path().join("workspace");
            fs::create_dir(&workspace).unwrap();
            let verifier_source = format!("{read_challenge}{body}");

            assert_eq!(
                run_independent_verifier(&workspace, &verifier_source, &environment).is_ok(),
                succeeds,
                "unexpected verifier result for {body:?}"
            );
        }
    }

    #[test]
    fn rust_owns_fixture_config_gateway_workspace_evidence_and_cleanup() {
        let repository = tempfile::tempdir().unwrap();
        let pi = repository.path().join("pi");
        fs::write(&pi, b"fake").unwrap();
        let mut runtime = FakeRuntime::ready();

        let evidence = run_with_runtime(
            request(repository.path(), AcceptancePhase::MacLocal),
            repository.path(),
            &mut runtime,
        )
        .expect("acceptance succeeds");

        assert_eq!(
            runtime.gateway_urls,
            [
                "http://127.0.0.1:11435/v1/models",
                "http://127.0.0.1:11435/loxa/status",
                "http://127.0.0.1:11435/v1/models",
                "http://127.0.0.1:11435/loxa/status",
            ]
        );
        assert_eq!(runtime.bridge_calls, 1);
        let config = String::from_utf8(runtime.config_bytes.unwrap()).unwrap();
        assert!(config.contains(r#""maxTokens": 4096"#));
        assert!(config.contains(r#""maxTokensField": "max_tokens""#));
        assert_eq!(evidence.phase, AcceptancePhase::MacLocal);
        assert!(runtime.temporary_root_is_canonical);
        assert!(runtime.temporary_root.is_some_and(|path| !path.exists()));
        let persisted = fs::read(
            repository
                .path()
                .join("target/pi-acceptance/test/evidence.json"),
        )
        .unwrap();
        assert_eq!(
            serde_json::from_slice::<SanitizedEvidence>(&persisted).unwrap(),
            evidence
        );
    }

    #[test]
    fn post_recovery_requires_and_preserves_exact_config_digest() {
        let repository = tempfile::tempdir().unwrap();
        fs::write(repository.path().join("pi"), b"fake").unwrap();
        let mut runtime = FakeRuntime::ready();
        let mut first = request(repository.path(), AcceptancePhase::MacLocal);
        first.evidence_dir = "target/pi-acceptance/first".into();
        let digest = run_with_runtime(first, repository.path(), &mut runtime)
            .unwrap()
            .provider_config_sha256;

        let mut recovery = request(repository.path(), AcceptancePhase::PostRecovery);
        recovery.expected_config_sha256 = Some(digest.clone());
        recovery.evidence_dir = "target/pi-acceptance/recovery".into();
        let mut runtime = FakeRuntime::ready();
        assert_eq!(
            run_with_runtime(recovery, repository.path(), &mut runtime)
                .unwrap()
                .provider_config_sha256,
            digest
        );

        let mut missing = request(repository.path(), AcceptancePhase::PostRecovery);
        missing.evidence_dir = "target/pi-acceptance/missing".into();
        let error =
            run_with_runtime(missing, repository.path(), &mut FakeRuntime::ready()).unwrap_err();
        assert!(error
            .to_string()
            .contains("expected provider config digest"));
    }

    #[test]
    fn rejects_endpoint_phase_output_limit_and_unsafe_evidence_inputs() {
        let repository = tempfile::tempdir().unwrap();
        fs::write(repository.path().join("pi"), b"fake").unwrap();

        let mut bad_endpoint = request(repository.path(), AcceptancePhase::MacLocal);
        bad_endpoint.base_url = "https://node.tail.ts.net/v1".into();
        assert!(
            run_with_runtime(bad_endpoint, repository.path(), &mut FakeRuntime::ready())
                .unwrap_err()
                .to_string()
                .contains("loopback")
        );

        let mut bad_limit = request(repository.path(), AcceptancePhase::MacLocal);
        bad_limit.max_tokens = 8192;
        assert!(
            run_with_runtime(bad_limit, repository.path(), &mut FakeRuntime::ready())
                .unwrap_err()
                .to_string()
                .contains("1 and 8191")
        );

        let mut escaped = request(repository.path(), AcceptancePhase::MacLocal);
        escaped.evidence_dir = "../evidence".into();
        assert!(
            run_with_runtime(escaped, repository.path(), &mut FakeRuntime::ready())
                .unwrap_err()
                .to_string()
                .contains("target/pi-acceptance")
        );
    }

    #[test]
    fn invalid_bridge_or_runtime_identity_never_publishes_evidence() {
        let repository = tempfile::tempdir().unwrap();
        fs::write(repository.path().join("pi"), b"fake").unwrap();
        let mut invalid_bridge = FakeRuntime::ready();
        invalid_bridge.gateway.truncate(2);
        struct BadBridge(FakeRuntime);
        impl AcceptanceRuntime for BadBridge {
            fn gateway_json(&mut self, url: &str) -> io::Result<Vec<u8>> {
                self.0.gateway_json(url)
            }

            fn run_bridge(&mut self, _invocation: &BridgeInvocation) -> io::Result<Vec<u8>> {
                Ok(br#"{"schemaVersion":1,"toolTrace":[]}"#.to_vec())
            }
        }
        let error = run_with_runtime(
            request(repository.path(), AcceptancePhase::MacLocal),
            repository.path(),
            &mut BadBridge(invalid_bridge),
        )
        .unwrap_err();
        assert!(error.to_string().contains("tool loop"));
        assert!(!repository
            .path()
            .join("target/pi-acceptance/test/evidence.json")
            .exists());

        let mut wrong_identity = FakeRuntime::ready();
        wrong_identity.gateway[1] = br#"{"health":"ready","model":"loxa","engine":{"name":"llama-cpp","version":"version: 10108 (wrong)"}}"#.to_vec();
        let mut wrong_request = request(repository.path(), AcceptancePhase::MacLocal);
        wrong_request.evidence_dir = "target/pi-acceptance/wrong-identity".into();
        let error =
            run_with_runtime(wrong_request, repository.path(), &mut wrong_identity).unwrap_err();
        assert!(error.to_string().contains("qualified"));
        assert_eq!(wrong_identity.bridge_calls, 0);
    }

    #[test]
    fn evidence_publication_never_overwrites_an_existing_artifact() {
        let repository = tempfile::tempdir().unwrap();
        fs::write(repository.path().join("pi"), b"fake").unwrap();
        let directory = repository.path().join("target/pi-acceptance/test");
        fs::create_dir_all(&directory).unwrap();
        fs::write(directory.join("evidence.json"), b"existing evidence\n").unwrap();

        let error = run_with_runtime(
            request(repository.path(), AcceptancePhase::MacLocal),
            repository.path(),
            &mut FakeRuntime::ready(),
        )
        .unwrap_err();

        assert!(error.to_string().contains("already exists"));
        assert_eq!(
            fs::read(directory.join("evidence.json")).unwrap(),
            b"existing evidence\n"
        );
        assert_eq!(fs::read_dir(directory).unwrap().count(), 1);
    }

    #[test]
    fn exact_workspace_rejects_extra_and_deleted_paths() {
        let (directory, seed, workspace) = exact_workspace_fixture();
        fs::write(workspace.join("extra.txt"), b"extra\n").unwrap();
        assert!(validate_exact_workspace(&seed, &workspace)
            .unwrap_err()
            .to_string()
            .contains("extra, deleted, or renamed"));

        fs::remove_dir_all(&workspace).unwrap();
        fs::create_dir(&workspace).unwrap();
        copy_tree(&seed, &workspace).unwrap();
        fs::write(workspace.join("src/merge-ranges.mjs"), PI_EXPECTED_SOURCE).unwrap();
        fs::remove_file(workspace.join("test/verify.mjs")).unwrap();
        assert!(validate_exact_workspace(&seed, &workspace)
            .unwrap_err()
            .to_string()
            .contains("extra, deleted, or renamed"));
        drop(directory);
    }

    #[test]
    fn exact_workspace_accepts_an_alternative_source_only_repair() {
        let (_directory, seed, workspace) = exact_workspace_fixture();
        fs::write(
            workspace.join("src/merge-ranges.mjs"),
            b"export function mergeRanges(ranges) { return ranges.map((range) => [...range]); }\n",
        )
        .unwrap();

        validate_exact_workspace(&seed, &workspace)
            .expect("the final verifier, not hidden source bytes, defines correctness");
    }

    #[cfg(unix)]
    #[test]
    fn exact_workspace_rejects_symlink_mode_and_hardlink_changes() {
        use std::os::unix::fs::{symlink, PermissionsExt};

        let (_directory, seed, workspace) = exact_workspace_fixture();
        fs::remove_file(workspace.join("test/verify.mjs")).unwrap();
        symlink(
            seed.join("test/verify.mjs"),
            workspace.join("test/verify.mjs"),
        )
        .unwrap();
        assert!(validate_exact_workspace(&seed, &workspace)
            .unwrap_err()
            .to_string()
            .contains("symbolic link"));

        let (_directory, seed, workspace) = exact_workspace_fixture();
        let source = workspace.join("test/verify.mjs");
        let mut permissions = fs::metadata(&source).unwrap().permissions();
        permissions.set_mode(permissions.mode() ^ 0o100);
        fs::set_permissions(source, permissions).unwrap();
        assert!(validate_exact_workspace(&seed, &workspace)
            .unwrap_err()
            .to_string()
            .contains("mode change"));

        let (_directory, seed, workspace) = exact_workspace_fixture();
        fs::hard_link(
            workspace.join("test/verify.mjs"),
            workspace.join("test/verify-hardlink.mjs"),
        )
        .unwrap();
        assert!(validate_exact_workspace(&seed, &workspace)
            .unwrap_err()
            .to_string()
            .contains("hard-linked"));
    }

    fn exact_workspace_fixture() -> (tempfile::TempDir, PathBuf, PathBuf) {
        let directory = tempfile::tempdir().unwrap();
        let seed = directory.path().join("seed");
        let workspace = directory.path().join("workspace");
        fs::create_dir(&seed).unwrap();
        fs::create_dir(&workspace).unwrap();
        materialize_fixture(&seed).unwrap();
        copy_tree(&seed, &workspace).unwrap();
        fs::write(workspace.join("src/merge-ranges.mjs"), PI_EXPECTED_SOURCE).unwrap();
        validate_exact_workspace(&seed, &workspace).unwrap();
        (directory, seed, workspace)
    }

    #[test]
    fn bridge_outer_deadline_has_bounded_cleanup_headroom() {
        assert!(BRIDGE_OUTER_TIMEOUT > Duration::from_millis(PI_PROCESS_TIMEOUT_MS));
        assert!(BRIDGE_OUTER_TIMEOUT <= Duration::from_secs(130));
        assert!(INDEPENDENT_VERIFIER_TIMEOUT <= Duration::from_secs(10));
    }

    #[test]
    fn bridge_poll_error_returns_immediately_to_the_ownership_guard() {
        let started = Instant::now();
        let mut polls = 0;
        let error = poll_until_deadline::<()>(Instant::now() + Duration::from_secs(5), || {
            polls += 1;
            Err(io::Error::other("injected try_wait failure"))
        })
        .unwrap_err();
        assert_eq!(polls, 1);
        assert!(started.elapsed() < Duration::from_millis(100));
        assert!(error.to_string().contains("injected try_wait"));
    }

    #[test]
    fn bridge_poll_uses_the_original_setup_deadline() {
        let deadline = Instant::now() + Duration::from_millis(5);
        std::thread::sleep(Duration::from_millis(10));
        let started = Instant::now();
        let error = poll_until_deadline::<()>(deadline, || Ok(None)).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(started.elapsed() < Duration::from_millis(50));
    }

    #[cfg(unix)]
    #[test]
    fn bridge_setup_failure_and_timeout_kill_and_reap_the_exact_pid_boundedly() {
        fn spawn_sleep(stdout: Stdio) -> std::process::Child {
            Command::new("sh")
                .args(["-c", "exec sleep 10"])
                .stdin(Stdio::null())
                .stdout(stdout)
                .stderr(Stdio::piped())
                .spawn()
                .expect("spawn exact test child")
        }

        fn wait_until_gone(pid: u32) {
            let deadline = Instant::now() + Duration::from_secs(2);
            while Instant::now() < deadline {
                if !Command::new("/bin/kill")
                    .args(["-0", &pid.to_string()])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status()
                    .is_ok_and(|status| status.success())
                {
                    return;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            panic!("bridge child PID {pid} remained after bounded cleanup");
        }

        let missing_pipe = spawn_sleep(Stdio::null());
        let missing_pid = missing_pipe.id();
        let started = Instant::now();
        let error = wait_for_bridge(missing_pipe, Duration::from_secs(5)).unwrap_err();
        assert!(started.elapsed() < Duration::from_millis(250));
        assert!(error.to_string().contains("stdout"));
        wait_until_gone(missing_pid);

        let timed = spawn_sleep(Stdio::piped());
        let timed_pid = timed.id();
        let started = Instant::now();
        let error = wait_for_bridge(timed, Duration::from_millis(20)).unwrap_err();
        assert!(started.elapsed() < Duration::from_millis(250));
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        wait_until_gone(timed_pid);
    }

    #[cfg(unix)]
    #[test]
    fn bridge_timeout_cooperatively_cancels_and_reaps_a_real_descendant() {
        assert_bridge_timeout_reaps_descendant(true);
    }

    #[cfg(unix)]
    #[test]
    fn bridge_timeout_force_fallback_reaps_a_real_descendant() {
        assert_bridge_timeout_reaps_descendant(false);
    }

    #[cfg(unix)]
    #[test]
    fn owned_bridge_group_never_resolves_to_loxa_or_pid_one() {
        assert_eq!(checked_owned_bridge_group(1, 7), None);
        assert_eq!(checked_owned_bridge_group(7, 7), None);
        assert_eq!(checked_owned_bridge_group(9, 7), Some(9));
    }

    #[cfg(unix)]
    fn assert_bridge_timeout_reaps_descendant(cooperative: bool) {
        let directory = tempfile::tempdir().unwrap();
        let pi_file = directory.path().join("pi.pid");
        let descendant_file = directory.path().join("descendant.pid");
        let pi_script = directory.path().join("fake-pi.sh");
        fs::write(
            &pi_script,
            concat!(
                "sleep 10 </dev/null >/dev/null 2>&1 & descendant=$!\n",
                "echo \"$descendant\" > \"$1\"\n",
                "wait \"$descendant\"\n",
            ),
        )
        .unwrap();
        let script = if cooperative {
            concat!(
                "sh \"$3\" \"$2\" & pi=$!; ",
                "echo \"$pi\" > \"$1\"; ",
                "dd bs=1 count=1 of=/dev/null 2>/dev/null; ",
                "kill \"$pi\" 2>/dev/null; wait \"$pi\" 2>/dev/null; exit 0"
            )
        } else {
            concat!(
                "trap '' TERM; ",
                "sh \"$3\" \"$2\" & pi=$!; ",
                "echo \"$pi\" > \"$1\"; ",
                "while :; do sleep 10; done"
            )
        };
        let mut command = Command::new("sh");
        command
            .args(["-c", script, "loxa-pi-bridge-test"])
            .arg(&pi_file)
            .arg(&descendant_file)
            .arg(&pi_script)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0);
        let mut child = command.spawn().expect("spawn fake bridge with descendant");
        let bridge_pid = child.id();
        let marker_deadline = Instant::now() + Duration::from_secs(2);
        let pi_pid = read_recorded_pid(&pi_file, marker_deadline, &mut child);
        let descendant_pid = read_recorded_pid(&descendant_file, marker_deadline, &mut child);

        let error = wait_for_bridge(child, Duration::from_millis(100)).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert_pid_gone_or_cleanup(bridge_pid);
        assert_pid_gone_or_cleanup(pi_pid);
        assert_pid_gone_or_cleanup(descendant_pid);
    }

    #[cfg(unix)]
    fn read_recorded_pid(path: &Path, deadline: Instant, child: &mut std::process::Child) -> u32 {
        loop {
            if let Ok(contents) = fs::read_to_string(path) {
                if let Ok(pid) = contents.trim().parse::<u32>() {
                    return pid;
                }
            }
            if Instant::now() >= deadline {
                force_kill_bridge_tree(child);
                let _ = child.wait();
                panic!("fake bridge did not record an owned PID");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[cfg(unix)]
    fn assert_pid_gone_or_cleanup(pid: u32) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            if !Command::new("/bin/kill")
                .args(["-0", &pid.to_string()])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .is_ok_and(|status| status.success())
            {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let _ = Command::new("/bin/kill")
            .args(["-9", &pid.to_string()])
            .status();
        panic!("bridge-owned PID {pid} remained after bounded cleanup");
    }

    #[test]
    fn rust_builds_exact_platform_specific_isolated_environments() {
        let mac_source = BTreeMap::from([
            ("PATH".into(), OsString::from("/usr/bin:/bin")),
            ("LANG".into(), OsString::from("en_US.UTF-8")),
            ("OPENAI_API_KEY".into(), OsString::from("must-not-leak")),
        ]);
        let mac = isolated_environment_for(
            EnvironmentPlatform::Mac,
            "/private/tmp/run/home".as_ref(),
            "/private/tmp/run/pi-config".as_ref(),
            "/private/tmp/run/tmp".as_ref(),
            &mac_source,
        );
        assert_eq!(
            mac.keys().map(String::as_str).collect::<Vec<_>>(),
            [
                "HOME",
                "LANG",
                "PATH",
                "PI_CODING_AGENT_DIR",
                "PI_OFFLINE",
                "PI_SKIP_VERSION_CHECK",
                "PI_TELEMETRY",
                "TMPDIR",
                "XDG_CACHE_HOME",
                "XDG_CONFIG_HOME",
                "XDG_DATA_HOME",
            ]
        );
        assert!(!mac.contains_key("OPENAI_API_KEY"));

        let windows_source = BTreeMap::from([
            ("Path".into(), OsString::from(r"C:\Windows\System32")),
            ("SystemRoot".into(), OsString::from(r"C:\Windows")),
            (
                "ComSpec".into(),
                OsString::from(r"C:\Windows\System32\cmd.exe"),
            ),
            ("SECRET".into(), OsString::from("must-not-leak")),
        ]);
        let windows = isolated_environment_for(
            EnvironmentPlatform::Windows,
            r"C:\Temp\run\home".as_ref(),
            r"C:\Temp\run\pi-config".as_ref(),
            r"C:\Temp\run\tmp".as_ref(),
            &windows_source,
        );
        for required in [
            "APPDATA",
            "COMSPEC",
            "HOME",
            "HOMEDRIVE",
            "HOMEPATH",
            "LOCALAPPDATA",
            "PATH",
            "SYSTEMROOT",
            "TEMP",
            "TMP",
            "USERPROFILE",
        ] {
            assert!(windows.contains_key(required), "missing {required}");
        }
        assert_eq!(windows["HOMEDRIVE"], "C:");
        assert_eq!(windows["HOMEPATH"], r"\Temp\run\home");
        assert!(!windows.contains_key("SECRET"));
        assert_eq!(windows["APPDATA"], r"C:\Temp\run\home\AppData\Roaming");
    }
}
