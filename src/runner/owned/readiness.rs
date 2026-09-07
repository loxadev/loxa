use super::{exit_code, OwnedServer, StartOutcome, StartupStop};
use crate::runner::launch::{Launch, LaunchPolicy};
#[cfg(unix)]
use crate::runner::service_transport::{readiness_unix, UnixReadiness};
use reqwest::blocking::Client;
use serde::Deserialize;
use std::io::Read as _;
use std::path::Path;
use std::time::{Duration, Instant};

pub(in crate::runner) const MAX_MODELS_BODY: usize = 1024 * 1024;

pub(in crate::runner) fn readiness_client() -> Result<Client, String> {
    Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_millis(250))
        .timeout(Duration::from_secs(1))
        .build()
        .map_err(|error| error.to_string())
}

pub(in crate::runner) fn readiness(client: &Client, port: u16, id: &str) -> Result<bool, String> {
    let response = match client
        .get(format!("http://127.0.0.1:{port}/v1/models"))
        .send()
    {
        Ok(response) => response,
        Err(_) => return Ok(false),
    };
    if !response.status().is_success() {
        return Ok(false);
    }
    models_reader_has_alias(response, id)
}

pub(crate) fn probe_model_alias(port: u16, id: &str) -> Result<bool, String> {
    readiness(&readiness_client()?, port, id)
}

#[derive(Deserialize)]
struct Models {
    data: Vec<Model>,
}

#[derive(Deserialize)]
struct Model {
    id: String,
}

pub fn models_body_has_alias(body: &str, id: &str) -> bool {
    serde_json::from_str::<Models>(body)
        .is_ok_and(|models| models.data.iter().any(|model| model.id == id))
}

pub(in crate::runner) fn models_reader_has_alias(
    mut reader: impl std::io::Read,
    id: &str,
) -> Result<bool, String> {
    let mut body = Vec::new();
    reader
        .by_ref()
        .take((MAX_MODELS_BODY + 1) as u64)
        .read_to_end(&mut body)
        .map_err(|error| error.to_string())?;
    if body.len() > MAX_MODELS_BODY {
        return Err(format!(
            "/v1/models response is too large (limit {MAX_MODELS_BODY} bytes)"
        ));
    }
    let body = std::str::from_utf8(&body).map_err(|error| error.to_string())?;
    Ok(models_body_has_alias(body, id))
}

pub(super) enum RequestedStartOutcome {
    Continue,
    Completed(StartOutcome),
    CleanupFailed,
}

pub(super) fn requested_start_outcome<F>(
    server: &mut OwnedServer,
    stop: &F,
) -> Result<RequestedStartOutcome, String>
where
    F: Fn() -> Option<StartupStop>,
{
    let Some(stop) = stop() else {
        return Ok(RequestedStartOutcome::Continue);
    };
    match server.terminate() {
        Ok(()) => Ok(RequestedStartOutcome::Completed(stop.into())),
        Err(_cleanup) if matches!(stop, StartupStop::Interrupted(_)) => {
            Ok(RequestedStartOutcome::CleanupFailed)
        }
        Err(cleanup) => Err(cleanup),
    }
}

impl OwnedServer {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn wait_until_ready<F>(
        mut self,
        launch: &Launch,
        timeout: Duration,
        requested_port: u16,
        unix_socket: Option<&Path>,
        service_runtime: Option<&tokio::runtime::Handle>,
        client: Option<&Client>,
        child_pid: u32,
        stop: &F,
    ) -> Result<StartOutcome, String>
    where
        F: Fn() -> Option<StartupStop>,
    {
        let deadline = Instant::now() + timeout;
        loop {
            if let Err(error) = self.collect_announcements() {
                return self.fail_start(error);
            }
            match requested_start_outcome(&mut self, &stop)? {
                RequestedStartOutcome::Continue => {}
                RequestedStartOutcome::Completed(outcome) => return Ok(outcome),
                RequestedStartOutcome::CleanupFailed => {
                    return Ok(StartOutcome::CleanupFailed(Box::new(self)))
                }
            }
            let status = match self.child_mut().try_wait() {
                Ok(status) => status,
                Err(error) => return self.fail_start(error.to_string()),
            };
            if let Some(status) = status {
                let code = exit_code(status);
                if let Err(cleanup) = self.terminate() {
                    return self.finish_cleanup_failure(cleanup);
                }
                return Ok(StartOutcome::Exited(self.server_exit(code)));
            }
            if let Some(endpoint) = unix_socket {
                #[cfg(unix)]
                let readiness = readiness_unix(
                    service_runtime.expect("Unix launch has its owning runtime handle"),
                    endpoint,
                    &launch.id,
                    child_pid,
                    &stop,
                );
                #[cfg(not(unix))]
                let readiness = UnixReadinessPoll {
                    endpoint_identity: None,
                    outcome: Err("Unix engine endpoints are unsupported on this platform".into()),
                };
                #[cfg(unix)]
                if let Some(identity) = readiness.endpoint_identity {
                    if let Err(error) = self.record_unix_endpoint_identity(identity) {
                        return self.fail_start(error);
                    }
                }
                match readiness.outcome {
                    Ok(UnixReadiness::Ready) => {
                        if let Err(error) = self.collect_announcements() {
                            return self.fail_start(error);
                        }
                        match requested_start_outcome(&mut self, &stop)? {
                            RequestedStartOutcome::Continue => {}
                            RequestedStartOutcome::Completed(outcome) => return Ok(outcome),
                            RequestedStartOutcome::CleanupFailed => {
                                return Ok(StartOutcome::CleanupFailed(Box::new(self)))
                            }
                        }
                        return Ok(StartOutcome::Ready(Box::new(self)));
                    }
                    Ok(UnixReadiness::Pending) => {}
                    Ok(UnixReadiness::Stopped(stop)) => {
                        let stopped = || Some(stop);
                        match requested_start_outcome(&mut self, &stopped)? {
                            RequestedStartOutcome::Completed(outcome) => return Ok(outcome),
                            RequestedStartOutcome::CleanupFailed => {
                                return Ok(StartOutcome::CleanupFailed(Box::new(self)))
                            }
                            RequestedStartOutcome::Continue => {
                                unreachable!("readiness returned a concrete stop")
                            }
                        }
                    }
                    Err(error) => return self.fail_start(error),
                }
            }
            if let Some(port) = self.announced_port {
                if requested_port != 0 && port != requested_port {
                    return self.fail_start(format!(
                        "llama-server announced port {port}, expected {requested_port}"
                    ));
                }
                match readiness(
                    client.as_ref().expect("TCP launch has a readiness client"),
                    port,
                    &launch.id,
                ) {
                    Ok(true) => {
                        if let Err(error) = self.collect_announcements() {
                            return self.fail_start(error);
                        }
                        self.port = port;
                        if launch.policy != LaunchPolicy::Foreground {
                            match requested_start_outcome(&mut self, &stop)? {
                                RequestedStartOutcome::Continue => {}
                                RequestedStartOutcome::Completed(outcome) => return Ok(outcome),
                                RequestedStartOutcome::CleanupFailed => {
                                    return Ok(StartOutcome::CleanupFailed(Box::new(self)))
                                }
                            }
                        }
                        return Ok(StartOutcome::Ready(Box::new(self)));
                    }
                    Ok(false) => {}
                    Err(error) => return self.fail_start(error),
                }
            }
            if Instant::now() >= deadline {
                let error = if self.announced_port.is_none() {
                    format!(
                        "llama-server did not announce a listening endpoint within {} ms",
                        timeout.as_millis()
                    )
                } else {
                    format!(
                        "llama-server did not become ready within {} ms",
                        timeout.as_millis()
                    )
                };
                return self.fail_start(error);
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}
