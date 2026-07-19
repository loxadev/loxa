use loxa_core::engine::RuntimeBackendKind;
use loxa_node::{LifecycleEvent, LifecycleEventSink, NodePaths, RunTermination, ShutdownResult};
use std::io;

struct SilentEvents;

impl LifecycleEventSink for SilentEvents {
    fn emit(&mut self, _: LifecycleEvent) -> io::Result<()> {
        Ok(())
    }
}

fn parse_port<I>(arguments: I) -> Result<Option<u16>, String>
where
    I: IntoIterator<Item = String>,
{
    let mut arguments = arguments.into_iter();
    let mut port = None;
    while let Some(argument) = arguments.next() {
        if argument != "--port" || port.is_some() {
            return Err(format!("unsupported loxa-node argument: {argument}"));
        }
        let value = arguments
            .next()
            .ok_or_else(|| "--port requires a value".to_string())?;
        let parsed = value
            .parse::<u16>()
            .ok()
            .filter(|port| *port != 0)
            .ok_or_else(|| "--port must be between 1 and 65535".to_string())?;
        port = Some(parsed);
    }
    Ok(port)
}

fn run() -> ShutdownResult {
    let port = match parse_port(std::env::args().skip(1)) {
        Ok(port) => port,
        Err(error) => return ShutdownResult::Failed(io::Error::other(error)),
    };
    let paths = NodePaths::detect();
    let diagnostics = loxa_node::install_daemon_diagnostics(&paths.logs_dir);
    let result = loxa_node::serve_node_with_diagnostics_health(
        None,
        port,
        RuntimeBackendKind::LlamaCpp,
        &paths,
        &mut SilentEvents,
        diagnostics.health(),
    );
    let result_class = match &result {
        ShutdownResult::Stopped(RunTermination::RequestedStop) => "requested_stop",
        ShutdownResult::Stopped(RunTermination::Interrupted) => "interrupted",
        ShutdownResult::Stopped(RunTermination::Failed) => "failed",
        ShutdownResult::Stopped(RunTermination::RecoveryRequired) => "recovery_required",
        ShutdownResult::Failed(_) => "error",
        ShutdownResult::RequiresProcessExit(_) => "requires_process_exit",
    };
    tracing::info!(
        target: "loxa_node::shutdown",
        event_code = "shutdown.completed",
        component = "shutdown",
        result_class,
    );
    drop(diagnostics);
    result
}

fn main() {
    match run() {
        ShutdownResult::Stopped(RunTermination::RequestedStop | RunTermination::Interrupted) => {}
        ShutdownResult::Stopped(RunTermination::Failed) => std::process::exit(1),
        ShutdownResult::Stopped(RunTermination::RecoveryRequired) => std::process::exit(2),
        ShutdownResult::Failed(error) => {
            eprintln!("loxa-node: {error}");
            std::process::exit(2);
        }
        ShutdownResult::RequiresProcessExit(fatal) => (*fatal).exit(1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_only_an_optional_nonzero_port() {
        assert_eq!(parse_port(Vec::<String>::new()).unwrap(), None);
        assert_eq!(
            parse_port(["--port".into(), "8080".into()]).unwrap(),
            Some(8080)
        );
        for invalid in [
            vec!["--model".into(), "x".into()],
            vec!["--port".into()],
            vec!["--port".into(), "0".into()],
            vec![
                "--port".into(),
                "8080".into(),
                "--port".into(),
                "8081".into(),
            ],
        ] {
            assert!(parse_port(invalid).is_err());
        }
    }
}
