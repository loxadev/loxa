use loxa_core::engine::RuntimeBackendKind;
use loxa_node::{LifecycleEvent, LifecycleEventSink, NodePaths, RunTermination, ShutdownResult};
use std::io;

struct SilentEvents;

impl LifecycleEventSink for SilentEvents {
    fn emit(&mut self, _: LifecycleEvent) -> io::Result<()> {
        Ok(())
    }
}

fn parse_ports<I>(arguments: I) -> Result<(Option<u16>, Option<u16>), String>
where
    I: IntoIterator<Item = String>,
{
    let mut arguments = arguments.into_iter();
    let mut port = None;
    let mut inference_port = None;
    while let Some(argument) = arguments.next() {
        let target = match argument.as_str() {
            "--port" if port.is_none() => &mut port,
            "--inference-port" if inference_port.is_none() => &mut inference_port,
            _ => return Err(format!("unsupported loxa-node argument: {argument}")),
        };
        let value = arguments
            .next()
            .ok_or_else(|| format!("{argument} requires a value"))?;
        let parsed = value
            .parse::<u16>()
            .ok()
            .filter(|port| *port != 0)
            .ok_or_else(|| format!("{argument} must be between 1 and 65535"))?;
        if target.replace(parsed).is_some() {
            return Err(format!("unsupported loxa-node argument: {argument}"));
        }
    }
    Ok((port, inference_port))
}

fn run() -> ShutdownResult {
    let (port, inference_port) = match parse_ports(std::env::args().skip(1)) {
        Ok(ports) => ports,
        Err(error) => return ShutdownResult::Failed(io::Error::other(error)),
    };
    let paths = NodePaths::detect();
    let diagnostics = loxa_node::install_daemon_diagnostics(&paths.logs_dir);
    let result = loxa_node::serve_node_with_diagnostics_health(
        None,
        port,
        inference_port,
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
        assert_eq!(parse_ports(Vec::<String>::new()).unwrap(), (None, None));
        assert_eq!(
            parse_ports(["--port".into(), "8080".into()]).unwrap(),
            (Some(8080), None)
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
            assert!(parse_ports(invalid).is_err());
        }
    }

    #[test]
    fn accepts_an_optional_nonzero_inference_port() {
        assert_eq!(parse_ports(Vec::<String>::new()).unwrap(), (None, None));
        assert_eq!(
            parse_ports([
                "--port".into(),
                "8080".into(),
                "--inference-port".into(),
                "8081".into(),
            ])
            .unwrap(),
            (Some(8080), Some(8081))
        );
        for invalid in [
            vec!["--inference-port".into()],
            vec!["--inference-port".into(), "0".into()],
            vec![
                "--inference-port".into(),
                "8080".into(),
                "--inference-port".into(),
                "8081".into(),
            ],
        ] {
            assert!(parse_ports(invalid).is_err());
        }
    }
}
