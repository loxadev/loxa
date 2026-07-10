use super::{ManagedChild, SupervisorError};
use serde::Deserialize;
use std::net::{SocketAddr, TcpListener};
use std::thread;
use std::time::{Duration, Instant};
use sysinfo::{Pid, ProcessesToUpdate, System};

pub const PROCESS_IDENTITY_TIMEOUT: Duration = Duration::from_secs(1);
pub const PROCESS_IDENTITY_POLL_INTERVAL: Duration = Duration::from_millis(25);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReadinessProbe {
    Ready,
    Loading,
    NotReady,
}

#[derive(Deserialize)]
struct ModelsResponse {
    data: Vec<ModelDescriptor>,
}

#[derive(Deserialize)]
struct ModelDescriptor {
    id: String,
}

pub struct LocalhostPortReservation {
    listener: TcpListener,
    port: u16,
}

pub fn reserve_localhost_port(
    requested: Option<u16>,
) -> Result<LocalhostPortReservation, SupervisorError> {
    let address = SocketAddr::from(([127, 0, 0, 1], requested.unwrap_or(0)));
    let listener = TcpListener::bind(address).map_err(|_| SupervisorError::NoFreePort)?;
    let port = listener
        .local_addr()
        .map_err(|_| SupervisorError::NoFreePort)?
        .port();
    Ok(LocalhostPortReservation { listener, port })
}

impl LocalhostPortReservation {
    pub fn port(&self) -> u16 {
        self.port
    }

    pub(super) fn release_for(self, expected: u16) -> Result<(), SupervisorError> {
        if self.port != expected {
            return Err(SupervisorError::RunStateConflict(format!(
                "reserved port {} does not match spawn port {expected}",
                self.port
            )));
        }
        drop(self.listener);
        Ok(())
    }
}

pub fn wait_for_generation_ready_or_exit<C: ManagedChild>(
    child: &mut C,
    port: u16,
    expected_alias: &str,
    timeout: Duration,
    interval: Duration,
) -> Result<(), SupervisorError> {
    let started = Instant::now();

    loop {
        return_if_child_exited(child)?;

        let Some(remaining) = remaining_before_deadline(started, timeout) else {
            return_if_child_exited(child)?;
            return Err(SupervisorError::HealthTimeout);
        };
        let request_timeout = request_timeout(remaining, interval);
        let probe =
            probe_generation_readiness(port, expected_alias, started, timeout, request_timeout);

        if probe == ReadinessProbe::Ready {
            return_if_child_exited(child)?;
            if remaining_before_deadline(started, timeout).is_some() {
                return Ok(());
            }
            return_if_child_exited(child)?;
            return Err(SupervisorError::HealthTimeout);
        }

        return_if_child_exited(child)?;
        let Some(remaining) = remaining_before_deadline(started, timeout) else {
            return_if_child_exited(child)?;
            return Err(SupervisorError::HealthTimeout);
        };
        let sleep_for = sleep_duration(remaining, interval);
        if !sleep_for.is_zero() {
            thread::sleep(sleep_for);
        }
    }
}

fn return_if_child_exited<C: ManagedChild>(child: &mut C) -> Result<(), SupervisorError> {
    if child.try_wait()?.is_some() {
        Err(SupervisorError::ChildExitedEarly(String::new()))
    } else {
        Ok(())
    }
}

fn probe_generation_readiness(
    port: u16,
    expected_alias: &str,
    started: Instant,
    timeout: Duration,
    request_timeout: Duration,
) -> ReadinessProbe {
    let url = format!("http://127.0.0.1:{port}/health");
    let Some(client) = localhost_client(request_timeout) else {
        return ReadinessProbe::NotReady;
    };
    let response = match client.get(url).send() {
        Ok(response) => response,
        Err(_) => return ReadinessProbe::NotReady,
    };

    if response.status().is_success() {
        return if models_contain_exact_alias(port, expected_alias, started, timeout) {
            ReadinessProbe::Ready
        } else {
            ReadinessProbe::NotReady
        };
    }
    if response.status().as_u16() == 503 {
        return ReadinessProbe::Loading;
    }
    if matches!(response.status().as_u16(), 404 | 405 | 501) {
        return if models_contain_exact_alias(port, expected_alias, started, timeout) {
            ReadinessProbe::Ready
        } else {
            ReadinessProbe::NotReady
        };
    }

    ReadinessProbe::NotReady
}

fn models_contain_exact_alias(
    port: u16,
    expected_alias: &str,
    started: Instant,
    timeout: Duration,
) -> bool {
    let Some(remaining) = remaining_before_deadline(started, timeout) else {
        return false;
    };
    let Some(client) = localhost_client(remaining) else {
        return false;
    };
    let Ok(response) = client
        .get(format!("http://127.0.0.1:{port}/v1/models"))
        .send()
    else {
        return false;
    };
    if !response.status().is_success() {
        return false;
    }
    let Ok(body) = response.text() else {
        return false;
    };
    let Ok(models) = serde_json::from_str::<ModelsResponse>(&body) else {
        return false;
    };
    models.data.len() == 1 && models.data[0].id == expected_alias
}

fn localhost_client(timeout: Duration) -> Option<reqwest::blocking::Client> {
    reqwest::blocking::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(timeout)
        .build()
        .ok()
}

fn remaining_before_deadline(started: Instant, timeout: Duration) -> Option<Duration> {
    let remaining = timeout.saturating_sub(started.elapsed());
    (!remaining.is_zero()).then_some(remaining)
}

fn request_timeout(remaining: Duration, interval: Duration) -> Duration {
    let poll_bound = if interval.is_zero() {
        Duration::from_millis(1)
    } else {
        interval
    };
    remaining.min(poll_bound).min(Duration::from_secs(1))
}

fn sleep_duration(remaining: Duration, interval: Duration) -> Duration {
    remaining.min(interval)
}

pub fn process_start_time_with_retry(pid: u32) -> Option<u64> {
    let started = Instant::now();
    process_start_time_with_retry_with(
        pid,
        PROCESS_IDENTITY_TIMEOUT,
        PROCESS_IDENTITY_POLL_INTERVAL,
        process_start_time,
        || started.elapsed(),
        thread::sleep,
    )
}

pub(super) fn process_start_time_with_retry_with<L, N, S>(
    pid: u32,
    timeout: Duration,
    interval: Duration,
    mut lookup: L,
    mut elapsed: N,
    mut sleep: S,
) -> Option<u64>
where
    L: FnMut(u32) -> Option<u64>,
    N: FnMut() -> Duration,
    S: FnMut(Duration),
{
    if let Some(start_time) = lookup(pid) {
        return Some(start_time);
    }
    if interval.is_zero() {
        return None;
    }

    let mut previous_elapsed = elapsed();
    if previous_elapsed >= timeout {
        return None;
    }

    loop {
        let remaining = timeout.saturating_sub(previous_elapsed);
        let sleep_for = interval.min(remaining);
        if sleep_for.is_zero() {
            return None;
        }
        sleep(sleep_for);

        let current_elapsed = elapsed();
        if current_elapsed <= previous_elapsed {
            return None;
        }

        if let Some(start_time) = lookup(pid) {
            return Some(start_time);
        }
        let refreshed_elapsed = elapsed();
        if refreshed_elapsed < current_elapsed || refreshed_elapsed <= previous_elapsed {
            return None;
        }
        if refreshed_elapsed >= timeout {
            return None;
        }
        previous_elapsed = refreshed_elapsed;
    }
}

fn process_start_time(pid: u32) -> Option<u64> {
    let mut system = System::new();
    let pid = Pid::from_u32(pid);
    system.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
    system.process(pid).map(|process| process.start_time())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry;
    use std::cell::{Cell, RefCell};
    use std::collections::VecDeque;
    use std::io::{self, Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Condvar, Mutex};
    use std::thread;
    use std::time::Duration;

    const EXPECTED_ALIAS: &str = "loxa-run-123-g0";

    #[test]
    fn identity_retry_recovers_after_two_transient_misses_with_two_bounded_sleeps() {
        let results = RefCell::new(VecDeque::from([None, None, Some(123)]));
        let lookups = Cell::new(0_u8);
        let elapsed = Cell::new(Duration::ZERO);
        let sleeps = RefCell::new(Vec::new());

        let result = process_start_time_with_retry_with(
            77,
            Duration::from_millis(100),
            Duration::from_millis(25),
            |pid| {
                assert_eq!(pid, 77);
                lookups.set(lookups.get() + 1);
                results.borrow_mut().pop_front().unwrap_or(None)
            },
            || elapsed.get(),
            |duration| {
                sleeps.borrow_mut().push(duration);
                elapsed.set(elapsed.get() + duration);
            },
        );

        assert_eq!(result, Some(123));
        assert_eq!(lookups.get(), 3);
        assert_eq!(
            sleeps.into_inner(),
            vec![Duration::from_millis(25), Duration::from_millis(25)]
        );
    }

    #[test]
    fn identity_retry_persistent_miss_stops_at_one_second_without_extra_sleep() {
        let lookups = Cell::new(0_u8);
        let elapsed = Cell::new(Duration::ZERO);
        let sleeps = RefCell::new(Vec::new());

        let result = process_start_time_with_retry_with(
            88,
            Duration::from_secs(1),
            Duration::from_millis(25),
            |_| {
                lookups.set(lookups.get() + 1);
                None
            },
            || elapsed.get(),
            |duration| {
                sleeps.borrow_mut().push(duration);
                elapsed.set(elapsed.get() + duration);
            },
        );

        assert_eq!(result, None);
        assert_eq!(elapsed.get(), Duration::from_secs(1));
        assert_eq!(lookups.get(), 41, "initial lookup plus the boundary lookup");
        assert_eq!(sleeps.borrow().len(), 40);
        assert!(sleeps
            .borrow()
            .iter()
            .all(|duration| *duration == Duration::from_millis(25)));
    }

    #[test]
    fn identity_retry_zero_interval_fails_after_the_immediate_lookup_without_spinning() {
        let lookups = Cell::new(0_u8);
        let sleeps = Cell::new(0_u8);

        let result = process_start_time_with_retry_with(
            99,
            Duration::from_secs(1),
            Duration::ZERO,
            |_| {
                lookups.set(lookups.get() + 1);
                None
            },
            || Duration::ZERO,
            |_| sleeps.set(sleeps.get() + 1),
        );

        assert_eq!(result, None);
        assert_eq!(lookups.get(), 1);
        assert_eq!(sleeps.get(), 0);
    }

    #[test]
    fn identity_retry_nonadvancing_clock_fails_after_one_sleep_without_spinning() {
        let lookups = Cell::new(0_u8);
        let sleeps = RefCell::new(Vec::new());

        let result = process_start_time_with_retry_with(
            101,
            Duration::from_secs(1),
            Duration::from_millis(25),
            |_| {
                lookups.set(lookups.get() + 1);
                None
            },
            || Duration::ZERO,
            |duration| sleeps.borrow_mut().push(duration),
        );

        assert_eq!(result, None);
        assert_eq!(lookups.get(), 1);
        assert_eq!(sleeps.into_inner(), vec![Duration::from_millis(25)]);
    }

    #[test]
    fn identity_retry_failed_lookup_that_reaches_deadline_stops_before_another_sleep() {
        let lookups = Cell::new(0_u8);
        let elapsed = Cell::new(Duration::ZERO);
        let sleeps = RefCell::new(Vec::new());

        let result = process_start_time_with_retry_with(
            102,
            Duration::from_secs(1),
            Duration::from_millis(25),
            |_| {
                lookups.set(lookups.get() + 1);
                if lookups.get() == 2 {
                    elapsed.set(Duration::from_secs(1));
                }
                None
            },
            || elapsed.get(),
            |duration| {
                sleeps.borrow_mut().push(duration);
                elapsed.set(elapsed.get() + duration);
            },
        );

        assert_eq!(result, None);
        assert_eq!(elapsed.get(), Duration::from_secs(1));
        assert_eq!(lookups.get(), 2);
        assert_eq!(sleeps.into_inner(), vec![Duration::from_millis(25)]);
    }

    #[test]
    fn identity_retry_returns_a_definitive_mismatch_without_retrying() {
        let results = RefCell::new(VecDeque::from([Some(222), Some(111)]));
        let lookups = Cell::new(0_u8);
        let sleeps = Cell::new(0_u8);

        let actual = process_start_time_with_retry_with(
            111,
            Duration::from_secs(1),
            Duration::from_millis(25),
            |_| {
                lookups.set(lookups.get() + 1);
                results.borrow_mut().pop_front().unwrap_or(None)
            },
            || Duration::ZERO,
            |_| sleeps.set(sleeps.get() + 1),
        );

        assert_ne!(actual, Some(111));
        assert_eq!(actual, Some(222));
        assert_eq!(lookups.get(), 1);
        assert_eq!(sleeps.get(), 0);
    }

    #[test]
    fn reservation_blocks_second_bind_until_consumed_at_spawn_boundary() {
        let reservation = reserve_localhost_port(None).expect("reserve localhost port");
        let address = SocketAddr::from(([127, 0, 0, 1], reservation.port()));

        assert!(TcpListener::bind(address).is_err());
        reservation
            .release_for(address.port())
            .expect("consume matching reservation");
        let rebound = TcpListener::bind(address).expect("rebind released localhost port");

        assert_eq!(rebound.local_addr().expect("rebound address"), address);
    }

    #[test]
    fn requested_port_reservation_uses_the_exact_requested_port() {
        let requested = TcpListener::bind(("127.0.0.1", 0))
            .expect("choose requested localhost port")
            .local_addr()
            .expect("requested localhost address")
            .port();

        let reservation =
            reserve_localhost_port(Some(requested)).expect("reserve requested localhost port");

        assert_ne!(requested, 0);
        assert_eq!(reservation.port(), requested);
    }

    #[test]
    fn reservation_port_mismatch_fails_before_os_spawn() {
        let reservation = reserve_localhost_port(None).expect("reserve localhost port");
        let mismatched_port = if reservation.port() == u16::MAX {
            reservation.port() - 1
        } else {
            reservation.port() + 1
        };
        let os_spawn_count = Cell::new(0_u8);

        let error = (|| -> Result<(), super::super::SupervisorError> {
            reservation.release_for(mismatched_port)?;
            os_spawn_count.set(os_spawn_count.get() + 1);
            Ok(())
        })()
        .expect_err("mismatched reservation must fail before OS spawn");

        assert!(matches!(
            error,
            super::super::SupervisorError::RunStateConflict(_)
        ));
        assert_eq!(os_spawn_count.get(), 0);
    }

    #[test]
    fn llama_server_args_uses_persisted_generation_alias_for_direct_model_spawn() {
        let alias = "loxa-run-123-g1";
        let args = super::super::llama_server_args(&super::super::ServerSpec {
            entry: registry::find("gemma-3-4b-it-q4").expect("registry entry"),
            model_path: PathBuf::from("/tmp/model.gguf"),
            llama_server_path: PathBuf::from("/tmp/llama-server"),
            port: 8080,
            ctx_tokens: super::super::DEFAULT_CTX_TOKENS,
            generation_alias: alias.to_string(),
        });

        assert_eq!(
            args,
            vec![
                "--model",
                "/tmp/model.gguf",
                "--alias",
                alias,
                "--host",
                "127.0.0.1",
                "--port",
                "8080",
                "--ctx-size",
                "8192",
                "--gpu-layers",
                "auto",
                "--flash-attn",
                "auto",
                "--jinja",
                "--metrics",
                "--log-disable",
            ]
        );
        assert_eq!(
            args.iter().filter(|arg| arg.as_str() == "--jinja").count(),
            1
        );
        assert!(!args.iter().any(|arg| arg == "--models-preset"));
        assert!(!args.iter().any(|arg| arg.starts_with("--router")));
    }

    #[test]
    fn health_503_is_loading_without_querying_models_on_that_poll() {
        let server = ScriptedHttpServer::spawn(vec![
            ("/health", ResponseAction::status(503)),
            ("/v1/models", ResponseAction::models(&[EXPECTED_ALIAS])),
        ]);
        let mut child = FakeChild::running();

        let error = wait_under_test(
            &mut child,
            server.port(),
            EXPECTED_ALIAS,
            Duration::from_millis(200),
            Duration::from_millis(20),
        )
        .expect_err("503 health must remain pending");

        assert!(matches!(error, SupervisorError::HealthTimeout));
        let trace = server.trace();
        assert!(trace.iter().any(|event| event == "request /health"));
        assert!(!trace.iter().any(|event| event.contains("/v1/models")));
    }

    #[test]
    fn health_200_requires_one_exact_generation_alias() {
        let server = ready_server(200, &[EXPECTED_ALIAS]);
        let mut child = FakeChild::running();

        let result = wait_under_test(
            &mut child,
            server.port(),
            EXPECTED_ALIAS,
            Duration::from_secs(2),
            Duration::from_secs(1),
        );
        assert!(
            result.is_ok(),
            "exact generation alias must be ready: {result:?}; trace: {:?}",
            server.trace()
        );

        assert_eq!(
            server.trace(),
            vec![
                "request /health",
                "response /health 200",
                "request /v1/models",
                "response /v1/models 200",
            ]
        );
    }

    #[test]
    fn health_200_with_wrong_alias_times_out() {
        let server = ready_server(200, &["loxa-run-other-g0"]);
        assert_times_out(&server, EXPECTED_ALIAS);
    }

    #[test]
    fn prefix_and_substring_generation_aliases_do_not_match() {
        for advertised in ["loxa-run-123", "loxa-run-123-g0-extra", "run-123-g0"] {
            let server = ready_server(200, &[advertised]);
            assert_times_out(&server, EXPECTED_ALIAS);
        }
    }

    #[test]
    fn unsupported_health_statuses_fall_back_to_the_exact_alias() {
        for status in [404, 405, 501] {
            let server = ready_server(status, &[EXPECTED_ALIAS]);
            let mut child = FakeChild::running();

            wait_under_test(
                &mut child,
                server.port(),
                EXPECTED_ALIAS,
                Duration::from_millis(500),
                Duration::from_millis(50),
            )
            .unwrap_or_else(|error| panic!("status {status} exact fallback failed: {error}"));

            assert_eq!(
                server.trace(),
                vec![
                    "request /health",
                    &format!("response /health {status}"),
                    "request /v1/models",
                    "response /v1/models 200",
                ]
            );
        }
    }

    #[test]
    fn unsupported_health_fallback_rejects_a_wrong_alias() {
        for status in [404, 405, 501] {
            let server = ready_server(status, &["loxa-run-other-g0"]);
            assert_times_out(&server, EXPECTED_ALIAS);
        }
    }

    #[test]
    fn malformed_models_json_is_not_ready() {
        let server = ScriptedHttpServer::spawn(vec![
            ("/health", ResponseAction::status(200)),
            ("/v1/models", ResponseAction::response(200, "{malformed")),
        ]);
        assert_times_out(&server, EXPECTED_ALIAS);
    }

    #[test]
    fn empty_models_data_is_not_ready() {
        let server = ready_server(200, &[]);
        assert_times_out(&server, EXPECTED_ALIAS);
    }

    #[test]
    fn multiple_models_are_not_ready_even_when_one_alias_is_exact() {
        let server = ready_server(200, &[EXPECTED_ALIAS, "unrelated-model"]);
        assert_times_out(&server, EXPECTED_ALIAS);
    }

    #[test]
    fn non_success_models_status_is_not_ready() {
        let server = ScriptedHttpServer::spawn(vec![
            ("/health", ResponseAction::status(200)),
            ("/v1/models", ResponseAction::response(503, "{}")),
        ]);
        assert_times_out(&server, EXPECTED_ALIAS);
    }

    #[test]
    fn health_connection_error_is_not_ready_and_does_not_query_models() {
        let server = ScriptedHttpServer::spawn(vec![
            ("/health", ResponseAction::Close),
            ("/v1/models", ResponseAction::models(&[EXPECTED_ALIAS])),
        ]);
        assert_times_out(&server, EXPECTED_ALIAS);
        assert!(!server
            .trace()
            .iter()
            .any(|event| event.contains("/v1/models")));
    }

    #[test]
    fn other_health_status_is_not_ready_and_does_not_query_models() {
        let server = ScriptedHttpServer::spawn(vec![
            ("/health", ResponseAction::status(500)),
            ("/v1/models", ResponseAction::models(&[EXPECTED_ALIAS])),
        ]);
        assert_times_out(&server, EXPECTED_ALIAS);
        assert!(!server
            .trace()
            .iter()
            .any(|event| event.contains("/v1/models")));
    }

    #[test]
    fn health_redirect_to_healthy_target_does_not_advance_readiness() {
        let server = ScriptedHttpServer::spawn(vec![
            (
                "/health",
                ResponseAction::redirect(302, "/redirected-health"),
            ),
            ("/redirected-health", ResponseAction::status(200)),
            ("/v1/models", ResponseAction::models(&[EXPECTED_ALIAS])),
        ]);

        assert_times_out(&server, EXPECTED_ALIAS);
        let trace = server.trace();
        assert!(!trace
            .iter()
            .any(|event| event.contains("/redirected-health")));
        assert!(!trace.iter().any(|event| event.contains("/v1/models")));
    }

    #[test]
    fn models_redirect_to_exact_alias_json_does_not_advance_readiness() {
        let server = ScriptedHttpServer::spawn(vec![
            ("/health", ResponseAction::status(200)),
            (
                "/v1/models",
                ResponseAction::redirect(307, "/redirected-models"),
            ),
            (
                "/redirected-models",
                ResponseAction::models(&[EXPECTED_ALIAS]),
            ),
        ]);

        assert_times_out(&server, EXPECTED_ALIAS);
        assert!(!server
            .trace()
            .iter()
            .any(|event| event.contains("/redirected-models")));
    }

    #[test]
    fn models_connection_error_is_not_ready() {
        let server = ScriptedHttpServer::spawn(vec![
            ("/health", ResponseAction::status(200)),
            ("/v1/models", ResponseAction::Close),
        ]);
        assert_times_out(&server, EXPECTED_ALIAS);
    }

    #[test]
    fn child_exit_before_probe_wins_without_requests_or_diagnostics() {
        let server = ready_server(200, &[EXPECTED_ALIAS]);
        let mut child = FakeChild::with_wait_results([Some(1)]);

        let error = wait_under_test(
            &mut child,
            server.port(),
            EXPECTED_ALIAS,
            Duration::from_millis(500),
            Duration::from_millis(50),
        )
        .expect_err("exited child must win before probing");

        assert!(matches!(error, SupervisorError::ChildExitedEarly(message) if message.is_empty()));
        assert!(server.trace().is_empty());
        assert_eq!(child.events, vec!["try_wait"]);
    }

    #[test]
    fn child_exit_after_complete_exact_alias_response_wins_before_ready() {
        let gate = ResponseGate::new();
        let server = ScriptedHttpServer::spawn(vec![
            ("/health", ResponseAction::status(200)),
            (
                "/v1/models",
                ResponseAction::models(&[EXPECTED_ALIAS]).with_gate(gate.clone()),
            ),
        ]);
        let mut child = FakeChild::exit_on_wait_after_response(2, gate);

        let error = wait_under_test(
            &mut child,
            server.port(),
            EXPECTED_ALIAS,
            Duration::from_millis(500),
            Duration::from_millis(50),
        )
        .expect_err("child exit after the full alias response must win");

        assert!(matches!(error, SupervisorError::ChildExitedEarly(message) if message.is_empty()));
        assert_eq!(child.events, vec!["try_wait", "try_wait"]);
        assert_eq!(
            server.trace(),
            vec![
                "request /health",
                "response /health 200",
                "request /v1/models",
                "response /v1/models 200",
            ]
        );
    }

    #[test]
    fn pending_probe_checks_child_again_before_another_poll() {
        let server = ScriptedHttpServer::spawn(vec![("/health", ResponseAction::status(503))]);
        let mut child = FakeChild::with_wait_results([None, Some(1)]);

        let error = wait_under_test(
            &mut child,
            server.port(),
            EXPECTED_ALIAS,
            Duration::from_millis(500),
            Duration::from_millis(200),
        )
        .expect_err("pending readiness must recheck child before sleeping");

        assert!(matches!(error, SupervisorError::ChildExitedEarly(message) if message.is_empty()));
        assert_eq!(
            server
                .trace()
                .iter()
                .filter(|event| *event == "request /health")
                .count(),
            1
        );
    }

    #[test]
    fn final_timeout_check_allows_child_exit_to_win() {
        let server = ready_server(200, &[EXPECTED_ALIAS]);
        let mut child = FakeChild::with_wait_results([None, Some(1)]);

        let error = wait_under_test(
            &mut child,
            server.port(),
            EXPECTED_ALIAS,
            Duration::ZERO,
            Duration::from_millis(5),
        )
        .expect_err("final child check must win at the deadline");

        assert!(matches!(error, SupervisorError::ChildExitedEarly(message) if message.is_empty()));
        assert!(server.trace().is_empty());
        assert_eq!(child.events, vec!["try_wait", "try_wait"]);
    }

    #[test]
    fn request_timeouts_and_sleeps_are_clamped_to_the_remaining_deadline() {
        let remaining = Duration::from_millis(3);
        let interval = Duration::from_secs(5);

        assert_eq!(request_timeout(remaining, interval), remaining);
        assert_eq!(sleep_duration(remaining, interval), remaining);
    }

    #[test]
    fn scripted_server_join_surfaces_handler_panics_outside_unwinding() {
        let handler = thread::spawn(|| panic!("injected scripted handler panic"));

        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            join_scripted_server(handler)
        }));

        assert!(panic.is_err());
    }

    fn wait_under_test<C: super::super::ManagedChild>(
        child: &mut C,
        port: u16,
        expected_alias: &str,
        timeout: Duration,
        interval: Duration,
    ) -> Result<(), SupervisorError> {
        super::wait_for_generation_ready_or_exit(child, port, expected_alias, timeout, interval)
    }

    fn assert_times_out(server: &ScriptedHttpServer, expected_alias: &str) {
        let mut child = FakeChild::running();
        let error = wait_under_test(
            &mut child,
            server.port(),
            expected_alias,
            Duration::from_millis(200),
            Duration::from_millis(20),
        )
        .expect_err("readiness must remain pending");
        assert!(
            matches!(error, SupervisorError::HealthTimeout),
            "unexpected readiness result: {error}; trace: {:?}",
            server.trace()
        );
        assert!(!child.events.contains(&"join_log_drains"));
        assert!(!child.events.contains(&"terminate"));
        assert!(!child.events.contains(&"kill"));
    }

    fn ready_server(health_status: u16, models: &[&str]) -> ScriptedHttpServer {
        ScriptedHttpServer::spawn(vec![
            ("/health", ResponseAction::status(health_status)),
            ("/v1/models", ResponseAction::models(models)),
        ])
    }

    #[derive(Clone)]
    enum ResponseAction {
        Response {
            status: u16,
            body: String,
            headers: Vec<(String, String)>,
            gate: Option<ResponseGate>,
        },
        Close,
    }

    impl ResponseAction {
        fn status(status: u16) -> Self {
            Self::response(status, "")
        }

        fn response(status: u16, body: impl Into<String>) -> Self {
            Self::Response {
                status,
                body: body.into(),
                headers: Vec::new(),
                gate: None,
            }
        }

        fn redirect(status: u16, location: &str) -> Self {
            Self::response(status, "").with_header("Location", location)
        }

        fn models(ids: &[&str]) -> Self {
            let data = ids
                .iter()
                .map(|id| format!(r#"{{"id":{id:?},"object":"model"}}"#))
                .collect::<Vec<_>>()
                .join(",");
            Self::response(200, format!(r#"{{"object":"list","data":[{data}]}}"#))
        }

        fn with_gate(self, response_gate: ResponseGate) -> Self {
            match self {
                Self::Response {
                    status,
                    body,
                    headers,
                    gate: None,
                } => Self::Response {
                    status,
                    body,
                    headers,
                    gate: Some(response_gate),
                },
                other => other,
            }
        }

        fn with_header(self, name: &str, value: &str) -> Self {
            match self {
                Self::Response {
                    status,
                    body,
                    mut headers,
                    gate,
                } => {
                    headers.push((name.to_string(), value.to_string()));
                    Self::Response {
                        status,
                        body,
                        headers,
                        gate,
                    }
                }
                other => other,
            }
        }

        fn gate(&self) -> Option<ResponseGate> {
            match self {
                Self::Response { gate, .. } => gate.clone(),
                Self::Close => None,
            }
        }
    }

    #[derive(Clone)]
    struct ResponseGate {
        state: Arc<(Mutex<ResponseGateState>, Condvar)>,
    }

    #[derive(Default)]
    struct ResponseGateState {
        delivered: bool,
        released: bool,
    }

    impl ResponseGate {
        fn new() -> Self {
            Self {
                state: Arc::new((Mutex::new(ResponseGateState::default()), Condvar::new())),
            }
        }

        fn mark_delivered_and_wait(&self) {
            let (state, changed) = &*self.state;
            let mut state = state.lock().expect("lock response gate");
            state.delivered = true;
            changed.notify_all();
            while !state.released {
                state = changed.wait(state).expect("wait for response release");
            }
        }

        fn wait_until_delivered_then_release(&self) {
            let (state, changed) = &*self.state;
            let mut state = state.lock().expect("lock response gate");
            while !state.delivered {
                state = changed.wait(state).expect("wait for delivered response");
            }
            state.released = true;
            changed.notify_all();
        }

        fn release(&self) {
            let (state, changed) = &*self.state;
            let mut state = state.lock().expect("lock response gate");
            state.released = true;
            changed.notify_all();
        }
    }

    struct ScriptedHttpServer {
        port: u16,
        trace: Arc<Mutex<Vec<String>>>,
        stop: Arc<AtomicBool>,
        gates: Vec<ResponseGate>,
        handle: Option<thread::JoinHandle<()>>,
    }

    impl ScriptedHttpServer {
        fn spawn(routes: Vec<(&'static str, ResponseAction)>) -> Self {
            let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind scripted server");
            let port = listener.local_addr().expect("scripted address").port();
            listener
                .set_nonblocking(true)
                .expect("set scripted server nonblocking");
            let trace = Arc::new(Mutex::new(Vec::new()));
            let stop = Arc::new(AtomicBool::new(false));
            let server_trace = Arc::clone(&trace);
            let server_stop = Arc::clone(&stop);
            let gates = routes
                .iter()
                .filter_map(|(_path, action)| action.gate())
                .collect::<Vec<_>>();

            let handle = thread::spawn(move || {
                while !server_stop.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((mut stream, _address)) => {
                            let path = read_request_path(&mut stream);
                            record_trace(&server_trace, format!("request {path}"));
                            let action = routes
                                .iter()
                                .find(|(route, _action)| *route == path)
                                .map(|(_route, action)| action.clone())
                                .unwrap_or(ResponseAction::Close);
                            respond(&mut stream, &path, action, &server_trace);
                        }
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(1));
                        }
                        Err(_) => break,
                    }
                }
            });

            Self {
                port,
                trace,
                stop,
                gates,
                handle: Some(handle),
            }
        }

        fn port(&self) -> u16 {
            self.port
        }

        fn trace(&self) -> Vec<String> {
            self.trace.lock().expect("lock request trace").clone()
        }
    }

    impl Drop for ScriptedHttpServer {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            for gate in &self.gates {
                gate.release();
            }
            if let Some(handle) = self.handle.take() {
                join_scripted_server(handle);
            }
        }
    }

    fn join_scripted_server(handle: thread::JoinHandle<()>) {
        let result = handle.join();
        if !thread::panicking() {
            result.expect("scripted HTTP server handler panicked");
        }
    }

    fn read_request_path(stream: &mut TcpStream) -> String {
        let mut bytes = [0_u8; 1024];
        let _ = stream.set_read_timeout(Some(Duration::from_millis(100)));
        let read = stream.read(&mut bytes).unwrap_or(0);
        let request = String::from_utf8_lossy(&bytes[..read]);
        request
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .map(str::to_string)
            .unwrap_or_else(|| {
                if read == 0 {
                    "<eof>".to_string()
                } else {
                    format!("<invalid:{request:?}>")
                }
            })
    }

    fn respond(
        stream: &mut TcpStream,
        path: &str,
        action: ResponseAction,
        trace: &Arc<Mutex<Vec<String>>>,
    ) {
        let ResponseAction::Response {
            status,
            body,
            headers,
            gate,
        } = action
        else {
            record_trace(trace, format!("close {path}"));
            return;
        };
        record_trace(trace, format!("response {path} {status}"));
        let reason = match status {
            200 => "OK",
            404 => "Not Found",
            405 => "Method Not Allowed",
            501 => "Not Implemented",
            503 => "Service Unavailable",
            _ => "Scripted Status",
        };
        let headers = headers
            .iter()
            .map(|(name, value)| format!("{name}: {value}\r\n"))
            .collect::<String>();
        let response = format!(
            "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n{headers}\r\n{body}",
            body.len(),
        );
        stream
            .write_all(response.as_bytes())
            .expect("write scripted response");
        stream.flush().expect("flush scripted response");
        if let Some(gate) = gate {
            gate.mark_delivered_and_wait();
        }
    }

    fn record_trace(trace: &Arc<Mutex<Vec<String>>>, event: String) {
        trace.lock().expect("lock request trace").push(event);
    }

    struct FakeChild {
        events: Vec<&'static str>,
        wait_results: VecDeque<Option<i32>>,
        repeat: Option<i32>,
        response_exit: Option<(usize, ResponseGate)>,
        wait_calls: usize,
    }

    impl FakeChild {
        fn running() -> Self {
            Self {
                events: Vec::new(),
                wait_results: VecDeque::from([None]),
                repeat: None,
                response_exit: None,
                wait_calls: 0,
            }
        }

        fn with_wait_results(results: impl IntoIterator<Item = Option<i32>>) -> Self {
            let wait_results = results.into_iter().collect::<VecDeque<_>>();
            let repeat = wait_results.back().copied().flatten();
            Self {
                events: Vec::new(),
                wait_results,
                repeat,
                response_exit: None,
                wait_calls: 0,
            }
        }

        fn exit_on_wait_after_response(wait_call: usize, gate: ResponseGate) -> Self {
            Self {
                events: Vec::new(),
                wait_results: VecDeque::from([None, Some(1)]),
                repeat: Some(1),
                response_exit: Some((wait_call, gate)),
                wait_calls: 0,
            }
        }
    }

    impl super::super::ManagedChild for FakeChild {
        fn pid(&self) -> u32 {
            777
        }

        fn terminate(&mut self) -> io::Result<()> {
            self.events.push("terminate");
            Ok(())
        }

        fn kill(&mut self) -> io::Result<()> {
            self.events.push("kill");
            Ok(())
        }

        fn try_wait(&mut self) -> io::Result<Option<i32>> {
            self.events.push("try_wait");
            self.wait_calls += 1;
            if let Some((wait_call, gate)) = &self.response_exit {
                if *wait_call <= self.wait_calls {
                    gate.wait_until_delivered_then_release();
                    return Ok(Some(1));
                }
            }
            Ok(self.wait_results.pop_front().unwrap_or(self.repeat))
        }
    }

    impl super::super::LogDrainingChild for FakeChild {
        fn join_log_drains(&mut self) -> Result<(), SupervisorError> {
            self.events.push("join_log_drains");
            Ok(())
        }
    }
}
