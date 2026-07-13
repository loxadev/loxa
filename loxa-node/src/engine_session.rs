use loxa_core::supervisor::{ManagedRun, ManagedRunIdentity, ManagedServer};

/// One exact engine child and its committed generation metadata.
///
/// The long-lived gateway/owner loop stays outside this value. The session
/// retains the committed run identity only so every child action remains tied
/// to the exact generation; an unloaded node is represented by `None`.
pub(super) struct EngineSession<C> {
    child: C,
    run: ManagedRun,
    server: ManagedServer,
    process_label: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct EngineSessionCorrelationError(&'static str);

impl std::fmt::Display for EngineSessionCorrelationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for EngineSessionCorrelationError {}

impl<C> EngineSession<C> {
    pub(super) fn new(
        child: C,
        run: ManagedRun,
        server: ManagedServer,
        process_label: impl Into<String>,
        observed_child_pid: u32,
        observed_child_process_start_time_unix_s: u64,
    ) -> Result<Self, (C, EngineSessionCorrelationError)> {
        if run.child_pid != Some(observed_child_pid)
            || server.pid != observed_child_pid
            || run.child_process_start_time_unix_s != Some(observed_child_process_start_time_unix_s)
            || server.process_start_time_unix_s != Some(observed_child_process_start_time_unix_s)
            || run.model_id.as_deref() != Some(server.id.as_str())
            || run.port != server.port
            || run.run_id.is_empty()
        {
            return Err((
                child,
                EngineSessionCorrelationError(
                    "engine child, server, model, port, and committed run are not exactly correlated",
                ),
            ));
        }
        Ok(Self {
            child,
            run,
            server,
            process_label: process_label.into(),
        })
    }

    pub(super) fn identity(&self) -> ManagedRunIdentity {
        self.run.identity()
    }

    #[cfg(test)]
    pub(super) fn owner_identity(&self) -> (u32, u64) {
        (self.run.owner_pid, self.run.owner_process_start_time_unix_s)
    }

    #[cfg(test)]
    pub(super) fn generation(&self) -> u32 {
        self.run.generation
    }

    pub(super) fn run(&self) -> &ManagedRun {
        &self.run
    }

    pub(super) fn server(&self) -> &ManagedServer {
        &self.server
    }

    pub(super) fn process_label(&self) -> &str {
        &self.process_label
    }

    pub(super) fn child_mut(&mut self) -> &mut C {
        &mut self.child
    }

    pub(super) fn into_parts(self) -> (C, ManagedRun, ManagedServer, String) {
        (self.child, self.run, self.server, self.process_label)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use loxa_core::supervisor::{ManagedRun, ManagedServer, RunLifecycle};
    use std::path::PathBuf;

    #[test]
    fn session_owns_one_exact_engine_generation_without_replacing_node_owner_identity() {
        let run = ManagedRun {
            schema_version: loxa_core::supervisor::RUNTIME_STATE_SCHEMA_VERSION,
            run_id: "stable-node-owner".into(),
            model_id: Some("gemma-3-4b-it-q4".into()),
            owner_pid: 42,
            owner_process_start_time_unix_s: 100,
            stop_requested: false,
            lifecycle: RunLifecycle::Running,
            generation: 3,
            generation_alias: "loxa-stable-node-owner-g3".into(),
            control_port: Some(8080),
            port: 9_090,
            log_path: PathBuf::from("engine.log"),
            child_pid: Some(77),
            child_process_start_time_unix_s: Some(200),
            child_pgid: Some(77),
        };
        let server = ManagedServer {
            id: "gemma-3-4b-it-q4".into(),
            pid: 77,
            port: 9_090,
            model_path: PathBuf::from("model.gguf"),
            started_at_unix_s: 300,
            llama_server_version: "test".into(),
            process_start_time_unix_s: Some(200),
        };

        let session = EngineSession::new((), run.clone(), server, "llama-server", 77, 200)
            .expect("correlated session");

        assert_eq!(session.identity(), run.identity());
        assert_eq!(session.owner_identity(), (42, 100));
        assert_eq!(session.generation(), 3);
    }

    #[test]
    fn rejects_a_stale_child_or_server_before_session_ownership() {
        let run = ManagedRun {
            schema_version: loxa_core::supervisor::RUNTIME_STATE_SCHEMA_VERSION,
            run_id: "stable-node-owner".into(),
            model_id: Some("a".into()),
            owner_pid: 42,
            owner_process_start_time_unix_s: 100,
            stop_requested: false,
            lifecycle: RunLifecycle::Running,
            generation: 2,
            generation_alias: "loxa-stable-node-owner-g2".into(),
            control_port: Some(8080),
            port: 9090,
            log_path: PathBuf::from("engine.log"),
            child_pid: Some(77),
            child_process_start_time_unix_s: Some(200),
            child_pgid: Some(77),
        };
        let server = ManagedServer {
            id: "a".into(),
            pid: 77,
            port: 9090,
            model_path: PathBuf::from("a.gguf"),
            started_at_unix_s: 300,
            llama_server_version: "test".into(),
            process_start_time_unix_s: Some(200),
        };
        assert!(EngineSession::new((), run, server, "llama-server", 78, 200).is_err());
    }
}
