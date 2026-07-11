use super::CliPaths;
use loxa_core::calibration::{
    run_calibration, CalibrationCandidate, CalibrationOutcome, CandidateOwnership,
};
use loxa_core::evidence::write_evidence_atomic;
use loxa_core::hardware::HardwareReport;
use loxa_core::plan::{CandidateIdentity, ProviderKind, SamplingPolicy};
use loxa_core::provider::llama::LlamaAdapter;
use loxa_core::provider::ollama::{OllamaAdapter, OllamaPreflight};
use loxa_core::provider::{
    InvocationObservation, InvocationRequest, ProviderAdapter, ProviderError,
};
use loxa_core::registry;
use loxa_core::selector::{select_plan, SelectorVerdict};
use loxa_core::supervisor::{self, RunLifecycle, RuntimeStateRead};
use std::env;
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitCode, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const OLLAMA_URL: &str = "http://127.0.0.1:11434";
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(10);

pub(super) fn run<W: Write, E: Write>(
    managed_id: &str,
    ollama_model: &str,
    ctx: u32,
    confirm_exclusive: bool,
    paths: &CliPaths,
    stdout: &mut W,
    stderr: &mut E,
) -> io::Result<ExitCode> {
    let mut factory = ProductionFactory { paths };
    run_with_factory(
        BenchOptions {
            managed_id,
            ollama_model,
            ctx,
            confirm_exclusive,
        },
        &mut factory,
        stdout,
        stderr,
    )
}

struct BenchOptions<'a> {
    managed_id: &'a str,
    ollama_model: &'a str,
    ctx: u32,
    confirm_exclusive: bool,
}

trait CandidateFactory {
    fn attached(&mut self, model: &str, ctx: u32) -> Result<Box<dyn CalibrationCandidate>, String>;
    fn managed(&mut self, id: &str, ctx: u32) -> Result<Box<dyn CalibrationCandidate>, String>;
    fn available_memory(&mut self) -> u64;
    fn evidence_path(&mut self) -> io::Result<PathBuf>;
}

fn run_with_factory<W: Write, E: Write>(
    options: BenchOptions<'_>,
    factory: &mut dyn CandidateFactory,
    stdout: &mut W,
    stderr: &mut E,
) -> io::Result<ExitCode> {
    if !options.confirm_exclusive {
        writeln!(stderr, "bench requires --confirm-exclusive")?;
        return Ok(ExitCode::from(1));
    }
    if registry::find(options.managed_id).is_none() {
        writeln!(stderr, "unknown model id: {}", options.managed_id)?;
        return Ok(ExitCode::from(1));
    }

    let mut attached = match factory.attached(options.ollama_model, options.ctx) {
        Ok(candidate) => candidate,
        Err(error) => {
            writeln!(stderr, "bench setup failed: {error}")?;
            return Ok(ExitCode::from(1));
        }
    };
    let mut managed = match factory.managed(options.managed_id, options.ctx) {
        Ok(candidate) => candidate,
        Err(error) => {
            writeln!(stderr, "bench setup failed: {error}")?;
            return Ok(ExitCode::from(1));
        }
    };

    let outcome = run_calibration(managed.as_mut(), attached.as_mut(), || {
        factory.available_memory()
    });
    match outcome {
        CalibrationOutcome::Uncontrolled { reason } => {
            writeln!(stderr, "bench uncontrolled: {reason}")?;
            Ok(ExitCode::from(1))
        }
        CalibrationOutcome::Failed {
            mut evidence,
            reason,
        } => {
            evidence.verdict = Some(SelectorVerdict::NoVerifiedPlan);
            let path = factory.evidence_path()?;
            write_evidence_atomic(&path, &evidence)?;
            let (code, line) = render_verdict(&SelectorVerdict::NoVerifiedPlan, &path);
            writeln!(stdout, "{line}")?;
            writeln!(stderr, "bench failed: {reason}")?;
            Ok(ExitCode::from(code))
        }
        CalibrationOutcome::Completed { mut evidence } => {
            let verdict = select_plan(&evidence);
            evidence.verdict = Some(verdict.clone());
            let path = factory.evidence_path()?;
            write_evidence_atomic(&path, &evidence)?;
            let (code, line) = render_verdict(&verdict, &path);
            writeln!(stdout, "{line}")?;
            Ok(ExitCode::from(code))
        }
    }
}

fn render_verdict(verdict: &SelectorVerdict, path: &Path) -> (u8, String) {
    let path = path.display();
    match verdict {
        SelectorVerdict::Selected { candidate_id } => (
            0,
            format!("selected candidate={candidate_id} evidence={path}"),
        ),
        SelectorVerdict::NoVerifiedPlan => (1, format!("no-verified-plan evidence={path}")),
        SelectorVerdict::NoMaterialWinner { baseline_id } => (
            0,
            format!("no-material-winner baseline={baseline_id} evidence={path}"),
        ),
    }
}

struct ProductionFactory<'a> {
    paths: &'a CliPaths,
}

impl CandidateFactory for ProductionFactory<'_> {
    fn attached(&mut self, model: &str, ctx: u32) -> Result<Box<dyn CalibrationCandidate>, String> {
        let preflight =
            OllamaAdapter::preflight(OLLAMA_URL, model).map_err(|error| error.to_string())?;
        let identity = attached_identity(model, ctx, &preflight)?;
        Ok(Box::new(AttachedCandidate {
            adapter: OllamaAdapter::new(identity, OLLAMA_URL, model),
        }))
    }

    fn managed(&mut self, id: &str, ctx: u32) -> Result<Box<dyn CalibrationCandidate>, String> {
        let entry = registry::find(id).ok_or_else(|| format!("unknown model id: {id}"))?;
        let server = supervisor::detect_llama_server().map_err(|error| error.to_string())?;
        let provider_version =
            supervisor::llama_server_version(&server).map_err(|error| error.to_string())?;
        let identity = CandidateIdentity {
            candidate_id: format!("managed-{id}"),
            provider: ProviderKind::ManagedLlama,
            provider_version,
            engine_revision: Some(required_env("LOXA_MANAGED_ENGINE_REVISION")?),
            model_id: id.to_string(),
            artifact_digest: format!("sha256:{}", entry.sha256),
            tokenizer_digest: required_env("LOXA_MANAGED_TOKENIZER_DIGEST")?,
            chat_template_digest: required_env("LOXA_MANAGED_CHAT_TEMPLATE_DIGEST")?,
            context_tokens: ctx,
            required_free_memory_bytes: (entry.min_free_mem_gb as f64 * 1_000_000_000.0) as u64,
            sampling: deterministic_sampling(),
        };
        require_complete(&identity)?;
        Ok(Box::new(ManagedCandidate {
            identity,
            model_id: id.to_string(),
            ctx,
            state_path: self.paths.state_path.clone(),
            child: None,
            adapter: None,
        }))
    }

    fn available_memory(&mut self) -> u64 {
        let report = HardwareReport::detect();
        report.ram_total_bytes.saturating_sub(report.ram_used_bytes)
    }

    fn evidence_path(&mut self) -> io::Result<PathBuf> {
        let root = supervisor::runtime_dir()
            .parent()
            .map(Path::to_path_buf)
            .ok_or_else(|| io::Error::other("runtime directory has no parent"))?;
        let seconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(io::Error::other)?
            .as_secs();
        Ok(root
            .join("evidence")
            .join(format!("bench-{seconds}-{}.json", std::process::id())))
    }
}

fn attached_identity(
    model: &str,
    ctx: u32,
    preflight: &OllamaPreflight,
) -> Result<CandidateIdentity, String> {
    let required_free_memory_bytes = required_env("LOXA_OLLAMA_REQUIRED_FREE_MEMORY_BYTES")?
        .parse::<u64>()
        .map_err(|_| {
            "LOXA_OLLAMA_REQUIRED_FREE_MEMORY_BYTES must be a positive integer".to_string()
        })?;
    let identity = CandidateIdentity {
        candidate_id: format!("ollama-{model}"),
        provider: ProviderKind::Ollama,
        provider_version: preflight.provider_version.clone(),
        engine_revision: Some(required_env("LOXA_OLLAMA_ENGINE_REVISION")?),
        model_id: model.to_string(),
        artifact_digest: preflight.artifact_digest.clone(),
        tokenizer_digest: required_env("LOXA_OLLAMA_TOKENIZER_DIGEST")?,
        chat_template_digest: required_env("LOXA_OLLAMA_CHAT_TEMPLATE_DIGEST")?,
        context_tokens: ctx,
        required_free_memory_bytes,
        sampling: deterministic_sampling(),
    };
    require_complete(&identity)?;
    Ok(identity)
}

fn required_env(name: &str) -> Result<String, String> {
    env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("exact candidate identity is not configured: set {name}"))
}

fn require_complete(identity: &CandidateIdentity) -> Result<(), String> {
    let errors = identity.identity_errors();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "candidate identity is incomplete: {}",
            errors.join(", ")
        ))
    }
}

fn deterministic_sampling() -> SamplingPolicy {
    SamplingPolicy {
        temperature_milli: 0,
        top_p_milli: 1000,
        seed: 1,
    }
}

struct AttachedCandidate {
    adapter: OllamaAdapter,
}

impl ProviderAdapter for AttachedCandidate {
    fn identity(&self) -> &CandidateIdentity {
        self.adapter.identity()
    }
    fn inspect(&mut self) -> Result<(), ProviderError> {
        self.adapter.inspect()
    }
    fn invoke(
        &mut self,
        request: &InvocationRequest,
    ) -> Result<InvocationObservation, ProviderError> {
        self.adapter.invoke(request)
    }
}

impl CalibrationCandidate for AttachedCandidate {
    fn ownership(&self) -> CandidateOwnership {
        CandidateOwnership::Attached
    }
    fn prepare(&mut self) -> Result<(), ProviderError> {
        Ok(())
    }
    fn finish(&mut self) -> Result<(), ProviderError> {
        Err(ProviderError::Protocol(
            "attached candidate lifecycle is user-owned".into(),
        ))
    }
    fn isolation_check(&mut self) -> Result<(), ProviderError> {
        self.adapter.isolation_check()
    }
}

struct ManagedCandidate {
    identity: CandidateIdentity,
    model_id: String,
    ctx: u32,
    state_path: PathBuf,
    child: Option<Child>,
    adapter: Option<LlamaAdapter>,
}

impl ProviderAdapter for ManagedCandidate {
    fn identity(&self) -> &CandidateIdentity {
        &self.identity
    }
    fn inspect(&mut self) -> Result<(), ProviderError> {
        self.adapter_mut()?.inspect()
    }
    fn invoke(
        &mut self,
        request: &InvocationRequest,
    ) -> Result<InvocationObservation, ProviderError> {
        self.adapter_mut()?.invoke(request)
    }
}

impl CalibrationCandidate for ManagedCandidate {
    fn ownership(&self) -> CandidateOwnership {
        CandidateOwnership::Managed
    }

    fn prepare(&mut self) -> Result<(), ProviderError> {
        self.require_empty_state()?;
        let executable = env::current_exe().map_err(provider_io)?;
        let child = Command::new(executable)
            .args([
                "run",
                self.model_id.as_str(),
                "--ctx",
                &self.ctx.to_string(),
                "--no-restart",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(provider_io)?;
        self.child = Some(child);
        if let Err(error) = self.attach_ready_adapter() {
            return match self.finish() {
                Ok(()) => Err(error),
                Err(cleanup) => Err(ProviderError::Io(format!(
                    "{error}; managed cleanup failed: {cleanup}"
                ))),
            };
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<(), ProviderError> {
        let Some(child) = self.child.as_mut() else {
            return Ok(());
        };
        let owner_pid = child.id();
        match graceful_managed_cleanup(child, &self.state_path, owner_pid) {
            Ok(()) => {
                self.adapter = None;
                self.child = None;
                Ok(())
            }
            Err(graceful_error) => {
                force_managed_cleanup(child, &self.state_path, owner_pid).map_err(
                    |force_error| {
                        ProviderError::Io(format!(
                            "{graceful_error}; forced cleanup also failed: {force_error}"
                        ))
                    },
                )?;
                self.adapter = None;
                self.child = None;
                Err(ProviderError::Io(format!(
                    "{graceful_error}; exact managed process group required forced cleanup"
                )))
            }
        }
    }

    fn isolation_check(&mut self) -> Result<(), ProviderError> {
        self.require_empty_state()
    }
}

impl ManagedCandidate {
    fn attach_ready_adapter(&mut self) -> Result<(), ProviderError> {
        let child = self.child.as_mut().ok_or_else(|| {
            ProviderError::Protocol("managed child is missing during readiness".into())
        })?;
        let owner_pid = child.id();
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| ProviderError::Io("managed run did not expose startup output".into()))?;
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        let port = loop {
            line.clear();
            let read = reader.read_line(&mut line).map_err(provider_io)?;
            if read == 0 {
                let status = child.wait().map_err(provider_io)?;
                return Err(ProviderError::Io(format!(
                    "managed run exited before readiness: {status}"
                )));
            }
            if let Some(value) = line.trim().strip_prefix("port: ") {
                break value.parse::<u16>().map_err(|_| {
                    ProviderError::Protocol("managed run emitted an invalid port".into())
                })?;
            }
        };
        let run = exact_run(&self.state_path, owner_pid, &self.model_id, port)?;
        self.adapter = Some(LlamaAdapter::new(
            self.identity.clone(),
            format!("http://127.0.0.1:{port}"),
            run.generation_alias,
        ));
        Ok(())
    }
    fn adapter_mut(&mut self) -> Result<&mut LlamaAdapter, ProviderError> {
        self.adapter
            .as_mut()
            .ok_or_else(|| ProviderError::Protocol("managed candidate is not prepared".into()))
    }

    fn require_empty_state(&self) -> Result<(), ProviderError> {
        match supervisor::read_runtime_state(&self.state_path).map_err(provider_core)? {
            RuntimeStateRead::Missing => Ok(()),
            RuntimeStateRead::Loaded(runs) if runs.is_empty() => Ok(()),
            RuntimeStateRead::Loaded(_) => Err(ProviderError::Protocol(
                "managed runtime state is not empty".into(),
            )),
            RuntimeStateRead::Legacy(_) | RuntimeStateRead::Corrupt(_) => Err(
                ProviderError::Protocol("managed runtime state requires recovery".into()),
            ),
        }
    }
}

fn exact_run(
    state_path: &Path,
    owner_pid: u32,
    model_id: &str,
    port: u16,
) -> Result<supervisor::ManagedRun, ProviderError> {
    let RuntimeStateRead::Loaded(runs) =
        supervisor::read_runtime_state(state_path).map_err(provider_core)?
    else {
        return Err(ProviderError::Protocol(
            "managed runtime state is unavailable after readiness".into(),
        ));
    };
    if runs.len() != 1 {
        return Err(ProviderError::Protocol(
            "managed readiness requires exactly one runtime entry".into(),
        ));
    }
    let run = runs.into_iter().next().unwrap();
    let expected_alias = format!("loxa-{}-g{}", run.run_id, run.generation);
    let expected_pgid = run.child_pid.and_then(|pid| i32::try_from(pid).ok());
    if run.owner_pid != owner_pid
        || run.model_id != model_id
        || run.port != port
        || run.lifecycle != RunLifecycle::Running
        || run.generation != 0
        || run.generation_alias != expected_alias
        || run.child_pid.is_none()
        || run.child_process_start_time_unix_s.is_none()
        || run.child_pgid != expected_pgid
    {
        return Err(ProviderError::Protocol(
            "managed runtime identity does not match the ready child".into(),
        ));
    }
    Ok(run)
}

fn state_contains_owner(state_path: &Path, owner_pid: u32) -> Result<bool, ProviderError> {
    match supervisor::read_runtime_state(state_path).map_err(provider_core)? {
        RuntimeStateRead::Missing => Ok(false),
        RuntimeStateRead::Loaded(runs) => Ok(runs.iter().any(|run| run.owner_pid == owner_pid)),
        RuntimeStateRead::Legacy(_) | RuntimeStateRead::Corrupt(_) => Err(ProviderError::Protocol(
            "managed runtime state requires recovery".into(),
        )),
    }
}

fn graceful_managed_cleanup(
    child: &mut Child,
    state_path: &Path,
    owner_pid: u32,
) -> Result<(), ProviderError> {
    if child.try_wait().map_err(provider_io)?.is_none() {
        let status = Command::new("/bin/kill")
            .args(["-INT", &owner_pid.to_string()])
            .status()
            .map_err(provider_io)?;
        if !status.success() {
            return Err(ProviderError::Io(format!(
                "failed to send SIGINT to managed owner pid {owner_pid}"
            )));
        }
    }
    let deadline = Instant::now() + CLEANUP_TIMEOUT;
    let exit = loop {
        if let Some(status) = child.try_wait().map_err(provider_io)? {
            break status;
        }
        if Instant::now() >= deadline {
            return Err(ProviderError::Io(format!(
                "managed owner pid {owner_pid} did not exit after SIGINT"
            )));
        }
        thread::sleep(Duration::from_millis(50));
    };
    if exit.code() != Some(130) {
        return Err(ProviderError::Io(format!(
            "managed owner pid {owner_pid} exited unexpectedly: {exit}"
        )));
    }
    if state_contains_owner(state_path, owner_pid)? {
        return Err(ProviderError::Io(format!(
            "managed cleanup was not confirmed for owner pid {owner_pid}"
        )));
    }
    Ok(())
}

fn force_managed_cleanup(
    child: &mut Child,
    state_path: &Path,
    owner_pid: u32,
) -> Result<(), ProviderError> {
    let run = exact_owner_run(state_path, owner_pid)?;
    if let Some(run) = &run {
        if let Some(child_pid) = run.child_pid {
            let expected_start = run.child_process_start_time_unix_s.ok_or_else(|| {
                ProviderError::Io("managed state lacks child identity for forced cleanup".into())
            })?;
            let child_pgid = run.child_pgid.ok_or_else(|| {
                ProviderError::Io(
                    "managed state lacks child process group for forced cleanup".into(),
                )
            })?;
            if i32::try_from(child_pid).ok() != Some(child_pgid) {
                return Err(ProviderError::Io(
                    "refusing forced cleanup: managed child process group is not exact".into(),
                ));
            }
            let observed_start = supervisor::process_start_time_with_retry(child_pid);
            if observed_start == Some(expected_start) {
                signal_process_group(child_pgid, 9).map_err(provider_io)?;
            } else if observed_start.is_some() {
                return Err(ProviderError::Io(format!(
                    "refusing forced cleanup: pid {child_pid} identity changed"
                )));
            }
        }
    }

    if child.try_wait().map_err(provider_io)?.is_none() {
        child.kill().map_err(provider_io)?;
    }
    let _ = child.wait().map_err(provider_io)?;

    if let Some(run) = &run {
        if let Some(pgid) = run.child_pgid {
            let deadline = Instant::now() + CLEANUP_TIMEOUT;
            while process_group_is_alive(pgid) {
                if Instant::now() >= deadline {
                    return Err(ProviderError::Io(format!(
                        "managed process group {pgid} remained alive after forced cleanup"
                    )));
                }
                thread::sleep(Duration::from_millis(50));
            }
        }
        supervisor::remove_runtime_state_entry(state_path, &run.identity())
            .map_err(provider_core)?;
    }
    if state_contains_owner(state_path, owner_pid)? {
        return Err(ProviderError::Io(format!(
            "managed state remained after forced cleanup for owner pid {owner_pid}"
        )));
    }
    Ok(())
}

fn exact_owner_run(
    state_path: &Path,
    owner_pid: u32,
) -> Result<Option<supervisor::ManagedRun>, ProviderError> {
    match supervisor::read_runtime_state(state_path).map_err(provider_core)? {
        RuntimeStateRead::Missing => Ok(None),
        RuntimeStateRead::Loaded(runs) => {
            let mut matching = runs.into_iter().filter(|run| run.owner_pid == owner_pid);
            let run = matching.next();
            if matching.next().is_some() {
                return Err(ProviderError::Protocol(
                    "multiple managed entries match the exact owner pid".into(),
                ));
            }
            Ok(run)
        }
        RuntimeStateRead::Legacy(_) | RuntimeStateRead::Corrupt(_) => Err(ProviderError::Protocol(
            "managed runtime state requires recovery".into(),
        )),
    }
}

#[cfg(unix)]
fn signal_process_group(pgid: i32, signal: i32) -> io::Result<()> {
    if pgid <= 1 {
        return Err(io::Error::other("unsafe managed process group identity"));
    }
    unsafe extern "C" {
        fn kill(pid: i32, signal: i32) -> i32;
    }
    let result = unsafe { kill(-pgid, signal) };
    if result == 0 || !process_group_is_alive(pgid) {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(unix)]
fn process_group_is_alive(pgid: i32) -> bool {
    if pgid <= 1 {
        return false;
    }
    unsafe extern "C" {
        fn kill(pid: i32, signal: i32) -> i32;
    }
    unsafe { kill(-pgid, 0) == 0 }
}

#[cfg(not(unix))]
fn signal_process_group(_pgid: i32, _signal: i32) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "managed process-group cleanup requires Unix",
    ))
}

#[cfg(not(unix))]
fn process_group_is_alive(_pgid: i32) -> bool {
    false
}

fn provider_io(error: io::Error) -> ProviderError {
    ProviderError::Io(error.to_string())
}

fn provider_core(error: supervisor::SupervisorError) -> ProviderError {
    ProviderError::Io(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use loxa_core::provider::ToolCall;
    use serde_json::json;
    use std::cell::Cell;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    struct FakeCandidate {
        identity: CandidateIdentity,
        ownership: CandidateOwnership,
        delay: Duration,
        finish_error: bool,
        isolation_error: bool,
        prepared: Option<Rc<Cell<bool>>>,
    }

    impl ProviderAdapter for FakeCandidate {
        fn identity(&self) -> &CandidateIdentity {
            &self.identity
        }
        fn inspect(&mut self) -> Result<(), ProviderError> {
            Ok(())
        }
        fn invoke(
            &mut self,
            request: &InvocationRequest,
        ) -> Result<InvocationObservation, ProviderError> {
            thread::sleep(self.delay);
            let prompt = request
                .messages
                .last()
                .map(|message| message.content.as_str())
                .unwrap_or_default();
            let tool = request.tools.first().map(|tool| tool.name.as_str());
            let (content, tool_calls) = match tool {
                Some("lookup_ticket") => (
                    None,
                    vec![ToolCall {
                        id: Some("call-ticket".into()),
                        name: "lookup_ticket".into(),
                        arguments: json!({"ticket_id":"TICKET-42"}),
                    }],
                ),
                Some("weather") if prompt.contains("word ready") => (Some("ready".into()), vec![]),
                Some("weather") => {
                    let city = if prompt.contains("Tokyo") {
                        "Tokyo"
                    } else if prompt.contains("Madrid") {
                        "Madrid"
                    } else {
                        "Paris"
                    };
                    let arguments = if prompt.contains("celsius") {
                        json!({"city":city,"units":"celsius"})
                    } else {
                        json!({"city":city})
                    };
                    (
                        None,
                        vec![ToolCall {
                            id: Some("call-weather".into()),
                            name: "weather".into(),
                            arguments,
                        }],
                    )
                }
                _ => (Some("TICKET-42 resolved".into()), vec![]),
            };
            Ok(InvocationObservation {
                content,
                tool_calls,
                prompt_tokens: Some(10),
                completion_tokens: Some(5),
                ttft_ns: Some(1),
                total_duration_ns: 2,
                prompt_rate: None,
                decode_rate: None,
                raw_events: vec![],
            })
        }
    }

    impl CalibrationCandidate for FakeCandidate {
        fn ownership(&self) -> CandidateOwnership {
            self.ownership
        }
        fn prepare(&mut self) -> Result<(), ProviderError> {
            if let Some(prepared) = &self.prepared {
                prepared.set(true);
            }
            Ok(())
        }
        fn finish(&mut self) -> Result<(), ProviderError> {
            if self.finish_error {
                Err(ProviderError::Io("cleanup failed".into()))
            } else {
                Ok(())
            }
        }
        fn isolation_check(&mut self) -> Result<(), ProviderError> {
            if self.isolation_error {
                Err(ProviderError::Protocol("uncontrolled isolation".into()))
            } else {
                Ok(())
            }
        }
    }

    struct TestFactory {
        managed_delay: Duration,
        attached_delay: Duration,
        identities_complete: bool,
        managed_cleanup_error: bool,
        path: PathBuf,
    }

    impl TestFactory {
        fn new(managed_ms: u64, attached_ms: u64) -> Self {
            let sequence = TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            Self {
                managed_delay: Duration::from_millis(managed_ms),
                attached_delay: Duration::from_millis(attached_ms),
                identities_complete: true,
                managed_cleanup_error: false,
                path: env::temp_dir().join(format!(
                    "loxa-bench-test-{}-{sequence}.json",
                    std::process::id()
                )),
            }
        }
    }

    impl Drop for TestFactory {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    impl CandidateFactory for TestFactory {
        fn attached(
            &mut self,
            model: &str,
            ctx: u32,
        ) -> Result<Box<dyn CalibrationCandidate>, String> {
            Ok(Box::new(FakeCandidate {
                identity: test_identity(
                    "attached",
                    model,
                    ctx,
                    ProviderKind::Ollama,
                    self.identities_complete,
                ),
                ownership: CandidateOwnership::Attached,
                delay: self.attached_delay,
                finish_error: false,
                isolation_error: false,
                prepared: None,
            }))
        }
        fn managed(&mut self, id: &str, ctx: u32) -> Result<Box<dyn CalibrationCandidate>, String> {
            Ok(Box::new(FakeCandidate {
                identity: test_identity(
                    "managed",
                    id,
                    ctx,
                    ProviderKind::ManagedLlama,
                    self.identities_complete,
                ),
                ownership: CandidateOwnership::Managed,
                delay: self.managed_delay,
                finish_error: self.managed_cleanup_error,
                isolation_error: false,
                prepared: None,
            }))
        }
        fn available_memory(&mut self) -> u64 {
            10_000
        }
        fn evidence_path(&mut self) -> io::Result<PathBuf> {
            Ok(self.path.clone())
        }
    }

    fn test_identity(
        candidate_id: &str,
        model: &str,
        ctx: u32,
        provider: ProviderKind,
        complete: bool,
    ) -> CandidateIdentity {
        CandidateIdentity {
            candidate_id: candidate_id.into(),
            provider,
            provider_version: "1".into(),
            engine_revision: complete.then(|| "revision".into()),
            model_id: model.into(),
            artifact_digest:
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            tokenizer_digest: "sha256:tokenizer".into(),
            chat_template_digest: "sha256:template".into(),
            context_tokens: ctx,
            required_free_memory_bytes: 100,
            sampling: deterministic_sampling(),
        }
    }

    fn run_test(factory: &mut dyn CandidateFactory) -> (ExitCode, String, String) {
        let mut stdout = vec![];
        let mut stderr = vec![];
        let exit = run_with_factory(
            BenchOptions {
                managed_id: "gemma-3-4b-it-q4",
                ollama_model: "test:latest",
                ctx: 8192,
                confirm_exclusive: true,
            },
            factory,
            &mut stdout,
            &mut stderr,
        )
        .unwrap();
        (
            exit,
            String::from_utf8(stdout).unwrap(),
            String::from_utf8(stderr).unwrap(),
        )
    }

    #[test]
    fn bench_prints_selected_candidate_and_evidence_path() {
        let mut factory = TestFactory::new(8, 1);
        let expected_path = factory.path.clone();
        let (exit, stdout, stderr) = run_test(&mut factory);
        assert_eq!(exit, ExitCode::SUCCESS);
        assert!(stderr.is_empty());
        assert_eq!(
            stdout,
            format!(
                "selected candidate=attached evidence={}\n",
                expected_path.display()
            )
        );
        assert!(expected_path.is_file());
    }

    #[test]
    fn bench_prints_no_verified_plan_and_exits_one() {
        let mut factory = TestFactory::new(1, 1);
        factory.identities_complete = false;
        let expected_path = factory.path.clone();
        let (exit, stdout, _) = run_test(&mut factory);
        assert_eq!(exit, ExitCode::from(1));
        assert_eq!(
            stdout,
            format!("no-verified-plan evidence={}\n", expected_path.display())
        );
        assert!(expected_path.is_file());
    }

    #[test]
    fn bench_prints_no_material_winner_and_keeps_managed_baseline() {
        let mut factory = TestFactory::new(1, 8);
        let expected_path = factory.path.clone();
        let (exit, stdout, stderr) = run_test(&mut factory);
        assert_eq!(exit, ExitCode::SUCCESS);
        assert!(stderr.is_empty());
        assert_eq!(
            stdout,
            format!(
                "no-material-winner baseline=managed evidence={}\n",
                expected_path.display()
            )
        );
        assert!(expected_path.is_file());
    }

    struct FailingAttachedFactory {
        attached_called: bool,
        managed_called: bool,
    }

    impl CandidateFactory for FailingAttachedFactory {
        fn attached(
            &mut self,
            _model: &str,
            _ctx: u32,
        ) -> Result<Box<dyn CalibrationCandidate>, String> {
            self.attached_called = true;
            Err("attached isolation is uncontrolled".into())
        }
        fn managed(
            &mut self,
            _id: &str,
            _ctx: u32,
        ) -> Result<Box<dyn CalibrationCandidate>, String> {
            self.managed_called = true;
            Err("must not be called".into())
        }
        fn available_memory(&mut self) -> u64 {
            0
        }
        fn evidence_path(&mut self) -> io::Result<PathBuf> {
            unreachable!()
        }
    }

    #[test]
    fn attached_factory_failure_does_not_construct_managed_candidate() {
        let mut factory = FailingAttachedFactory {
            attached_called: false,
            managed_called: false,
        };
        let mut stdout = vec![];
        let mut stderr = vec![];
        let exit = run_with_factory(
            BenchOptions {
                managed_id: "gemma-3-4b-it-q4",
                ollama_model: "test",
                ctx: 8192,
                confirm_exclusive: true,
            },
            &mut factory,
            &mut stdout,
            &mut stderr,
        )
        .unwrap();
        assert_eq!(exit, ExitCode::from(1));
        assert!(!factory.managed_called);
        assert!(String::from_utf8(stderr)
            .unwrap()
            .contains("attached isolation is uncontrolled"));
    }

    struct UncontrolledFactory {
        managed_prepared: Rc<Cell<bool>>,
        path: PathBuf,
    }

    impl CandidateFactory for UncontrolledFactory {
        fn attached(
            &mut self,
            model: &str,
            ctx: u32,
        ) -> Result<Box<dyn CalibrationCandidate>, String> {
            Ok(Box::new(FakeCandidate {
                identity: test_identity("attached", model, ctx, ProviderKind::Ollama, true),
                ownership: CandidateOwnership::Attached,
                delay: Duration::ZERO,
                finish_error: false,
                isolation_error: true,
                prepared: None,
            }))
        }
        fn managed(&mut self, id: &str, ctx: u32) -> Result<Box<dyn CalibrationCandidate>, String> {
            Ok(Box::new(FakeCandidate {
                identity: test_identity("managed", id, ctx, ProviderKind::ManagedLlama, true),
                ownership: CandidateOwnership::Managed,
                delay: Duration::ZERO,
                finish_error: false,
                isolation_error: false,
                prepared: Some(self.managed_prepared.clone()),
            }))
        }
        fn available_memory(&mut self) -> u64 {
            10_000
        }
        fn evidence_path(&mut self) -> io::Result<PathBuf> {
            Ok(self.path.clone())
        }
    }

    #[test]
    fn bench_reports_uncontrolled_without_starting_managed_candidate() {
        let prepared = Rc::new(Cell::new(false));
        let mut factory = UncontrolledFactory {
            managed_prepared: prepared.clone(),
            path: env::temp_dir().join("loxa-uncontrolled-must-not-write.json"),
        };

        let (exit, stdout, stderr) = run_test(&mut factory);

        assert_eq!(exit, ExitCode::from(1));
        assert!(stdout.is_empty());
        assert!(stderr.contains("bench uncontrolled"));
        assert!(!prepared.get());
        assert!(!factory.path.exists());
    }

    #[test]
    fn bench_confirmation_and_registry_validation_precede_attached_preflight() {
        for (managed_id, confirmed, expected) in [
            ("missing-model", false, "bench requires --confirm-exclusive"),
            ("missing-model", true, "unknown model id: missing-model"),
        ] {
            let mut factory = FailingAttachedFactory {
                attached_called: false,
                managed_called: false,
            };
            let mut stdout = vec![];
            let mut stderr = vec![];
            let exit = run_with_factory(
                BenchOptions {
                    managed_id,
                    ollama_model: "unused",
                    ctx: 8192,
                    confirm_exclusive: confirmed,
                },
                &mut factory,
                &mut stdout,
                &mut stderr,
            )
            .unwrap();
            assert_eq!(exit, ExitCode::from(1));
            assert!(stdout.is_empty());
            assert!(String::from_utf8(stderr).unwrap().contains(expected));
            assert!(!factory.attached_called);
            assert!(!factory.managed_called);
        }
    }

    #[test]
    fn bench_managed_cleanup_failure_exits_one_and_preserves_evidence() {
        let mut factory = TestFactory::new(1, 1);
        factory.managed_cleanup_error = true;
        let expected_path = factory.path.clone();
        let (exit, stdout, stderr) = run_test(&mut factory);
        assert_eq!(exit, ExitCode::from(1));
        assert_eq!(
            stdout,
            format!("no-verified-plan evidence={}\n", expected_path.display())
        );
        assert!(stderr.contains("managed cleanup failed"));
        let persisted: loxa_core::calibration::CalibrationEvidence =
            serde_json::from_slice(&std::fs::read(&expected_path).unwrap()).unwrap();
        assert_eq!(persisted.verdict, Some(SelectorVerdict::NoVerifiedPlan));
        assert_eq!(
            persisted.managed.failure.as_deref(),
            Some("candidate hard-gate failure")
        );
    }

    #[cfg(unix)]
    #[test]
    fn forced_cleanup_kills_exact_managed_group_and_removes_state() {
        use std::os::unix::process::CommandExt;

        let sequence = TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let root = env::temp_dir().join(format!(
            "loxa-force-cleanup-test-{}-{sequence}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let state_path = root.join("managed.json");
        let mut child = Command::new("/bin/sh")
            .args(["-c", "trap '' INT; while :; do sleep 1; done"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .spawn()
            .unwrap();
        let pid = child.id();
        let started = supervisor::process_start_time_with_retry(pid).unwrap();
        let starting = supervisor::ManagedRun {
            schema_version: supervisor::RUNTIME_STATE_SCHEMA_VERSION,
            run_id: format!("run-{pid}-{started}"),
            model_id: "gemma-3-4b-it-q4".into(),
            owner_pid: pid,
            owner_process_start_time_unix_s: started,
            stop_requested: false,
            lifecycle: RunLifecycle::Starting,
            generation: 0,
            generation_alias: format!("loxa-run-{pid}-{started}-g0"),
            port: 65_000,
            log_path: root.join("managed.log"),
            child_pid: None,
            child_process_start_time_unix_s: None,
            child_pgid: None,
        };
        supervisor::create_starting_run(&state_path, starting.clone()).unwrap();
        let mut running = starting.clone();
        running.lifecycle = RunLifecycle::Running;
        running.child_pid = Some(pid);
        running.child_process_start_time_unix_s = Some(started);
        running.child_pgid = Some(pid as i32);
        assert!(
            supervisor::update_runtime_state_run(&state_path, &starting.identity(), running)
                .unwrap()
        );

        force_managed_cleanup(&mut child, &state_path, pid).unwrap();

        assert!(child.try_wait().unwrap().is_some());
        assert!(!process_group_is_alive(pid as i32));
        assert!(!state_contains_owner(&state_path, pid).unwrap());
        let _ = std::fs::remove_dir_all(root);
    }
}
