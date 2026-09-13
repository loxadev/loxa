use std::sync::Mutex;

use dispatch2::{DispatchQueue, DispatchTime, MainThreadBound};
use loxa::paths::AppPaths;
use loxa_ipc::ServiceClient;
use objc2::MainThreadMarker;
use tauri::tray::{MouseButton, MouseButtonState, TrayIcon, TrayIconBuilder, TrayIconEvent};
use tauri::{AppHandle, ExitRequestApi, Manager, RunEvent, Wry};

use crate::menu::macos::{NativeExitResources, NativePopoverController};
use crate::preferences::{PreferencesExit, PreferencesOwner, WeakPreferencesOwner};

#[derive(Clone)]
struct ApplicationLaunch {
    paths: AppPaths,
    service_client: Option<ServiceClient>,
}

impl ApplicationLaunch {
    fn from_process() -> Result<Self, String> {
        let executable = std::env::current_exe().map_err(|error| error.to_string())?;
        let normal_paths = AppPaths::from_application_env(&executable)?;
        let Some(service_root) = std::env::var_os("LOXA_SERVICE_DEV_ROOT") else {
            return Ok(Self {
                paths: normal_paths,
                service_client: None,
            });
        };
        let service_client = ServiceClient::load(
            std::path::Path::new(&service_root),
            Some(&normal_paths.root),
            loxa::service::BUILD_ID,
        )?;
        let service_paths = AppPaths::from_application_values(
            service_client.bootstrap().origin().executable(),
            Some(service_client.bootstrap().root().root()),
            None,
        )?;
        Ok(Self {
            paths: service_paths,
            service_client: Some(service_client),
        })
    }

    #[cfg(test)]
    fn from_values(
        executable: &std::path::Path,
        explicit: Option<&std::path::Path>,
        home: Option<&std::path::Path>,
    ) -> Result<Self, String> {
        Ok(Self {
            paths: AppPaths::from_application_values(executable, explicit, home)?,
            service_client: None,
        })
    }

    #[cfg(test)]
    fn worker_paths(&self) -> (AppPaths, AppPaths) {
        (self.paths.clone(), self.paths.clone())
    }

    fn into_worker_parts(self) -> (AppPaths, AppPaths, Option<ServiceClient>) {
        (self.paths.clone(), self.paths, self.service_client)
    }
}

struct NativeShell {
    controller: MainThreadBound<NativePopoverController>,
    tray: TrayIcon<Wry>,
}

impl NativeShell {
    fn teardown(self) {
        let mtm = MainThreadMarker::new()
            .expect("Tauri must deliver native shell teardown on the main thread");
        let Self { controller, tray } = self;

        drop(controller.into_inner(mtm));
        drop(tray);
    }
}

struct NativeShellState(Mutex<Option<NativeShell>>);

struct NativeExitAttempt {
    shell: NativeShell,
    resources: NativeExitResources,
    preferences: PreferencesOwner,
}

struct NativePreferencesState(PreferencesOwner);

struct NativeExitState(Mutex<NativeExitStateInner>);

struct NativeExitStateInner {
    phase: NativeExitPhase,
    attempt: Option<NativeExitAttempt>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NativeExitPhase {
    Idle,
    Preparing,
    Waiting,
    Completed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NativeExitAdmission {
    Proceed,
    Prevent,
    Allow,
}

impl NativeExitState {
    fn new() -> Self {
        Self(Mutex::new(NativeExitStateInner {
            phase: NativeExitPhase::Idle,
            attempt: None,
        }))
    }

    fn claim(&self) -> NativeExitAdmission {
        let mut state = self
            .0
            .lock()
            .expect("native exit state mutex must not be poisoned");
        match state.phase {
            NativeExitPhase::Idle => {
                state.phase = NativeExitPhase::Preparing;
                NativeExitAdmission::Proceed
            }
            NativeExitPhase::Preparing | NativeExitPhase::Waiting => NativeExitAdmission::Prevent,
            NativeExitPhase::Completed => NativeExitAdmission::Allow,
        }
    }

    fn wait_with(&self, attempt: NativeExitAttempt) {
        let mut state = self
            .0
            .lock()
            .expect("native exit state mutex must not be poisoned");
        debug_assert_eq!(state.phase, NativeExitPhase::Preparing);
        debug_assert!(state.attempt.is_none());
        state.attempt = Some(attempt);
        state.phase = NativeExitPhase::Waiting;
    }

    fn take_waiting(&self) -> Option<NativeExitAttempt> {
        let mut state = self
            .0
            .lock()
            .expect("native exit state mutex must not be poisoned");
        if state.phase != NativeExitPhase::Waiting {
            return None;
        }
        state.phase = NativeExitPhase::Preparing;
        state.attempt.take()
    }

    fn reset(&self) {
        let mut state = self
            .0
            .lock()
            .expect("native exit state mutex must not be poisoned");
        debug_assert!(state.attempt.is_none());
        state.phase = NativeExitPhase::Idle;
    }

    fn complete(&self) {
        let mut state = self
            .0
            .lock()
            .expect("native exit state mutex must not be poisoned");
        debug_assert!(state.attempt.is_none());
        state.phase = NativeExitPhase::Completed;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NativeShellLifecycleEvent {
    QuitRequested,
    ExitRequested,
}

fn dispatch_native_shell_lifecycle(
    event: NativeShellLifecycleEvent,
    request_exit: impl FnOnce(),
    teardown: impl FnOnce(),
) {
    match event {
        NativeShellLifecycleEvent::QuitRequested => request_exit(),
        NativeShellLifecycleEvent::ExitRequested => teardown(),
    }
}

fn run_exit_transaction<Resource>(
    mut resource: Resource,
    flush_preferences: impl FnOnce(&mut Resource) -> Result<(), ()>,
    shutdown_runtime: impl FnOnce(&mut Resource) -> Result<(), ()>,
    shutdown_backend: impl FnOnce(&mut Resource) -> Result<(), ()>,
    finish: impl FnOnce(Resource),
) -> Result<(), Resource> {
    if flush_preferences(&mut resource).is_err() {
        return Err(resource);
    }
    if shutdown_runtime(&mut resource).is_err() {
        return Err(resource);
    }
    if shutdown_backend(&mut resource).is_err() {
        return Err(resource);
    }
    finish(resource);
    Ok(())
}

pub(crate) fn request_native_shell_exit(app_handle: &AppHandle) {
    dispatch_native_shell_lifecycle(
        NativeShellLifecycleEvent::QuitRequested,
        || app_handle.exit(0),
        || {},
    );
}

pub(crate) fn run() -> Result<(), String> {
    let launch = ApplicationLaunch::from_process()
        .map_err(|error| format!("failed to resolve launch paths: {error}"))?;
    let (backend_paths, runtime_paths, service_client) = launch.into_worker_parts();
    let diagnostics =
        match loxa_diagnostics::init(&backend_paths.logs, loxa_diagnostics::ProcessRole::Desktop) {
            Ok(diagnostics) => Some(diagnostics),
            Err(_) => {
                eprintln!("Loxa desktop diagnostics are unavailable");
                None
            }
        };
    tracing::info!(target: "loxa_app", event = "desktop_startup");
    let app = crate::native_menu::with_native_edit_menu(tauri::Builder::default())
        .setup(move |app| {
            app.set_activation_policy(tauri::ActivationPolicy::Accessory);

            let preferences_directory = app.path().app_config_dir()?;
            std::fs::create_dir_all(&preferences_directory)?;
            let preferences =
                PreferencesOwner::open(&preferences_directory.join("desktop-preferences.json"))
                    .map_err(|error| std::io::Error::other(error.context))?;
            let notification_owner = preferences.downgrade();
            preferences.set_completion_notifier(std::sync::Arc::new(move || {
                schedule_preference_completion(notification_owner.clone());
            }));
            assert!(app.manage(NativePreferencesState(preferences)));
            assert!(app.manage(NativeExitState::new()));

            let tray = TrayIconBuilder::with_id("loxa")
                .icon(tauri::include_image!("./icons/loxa-template.png"))
                .icon_as_template(true)
                .tooltip("Loxa")
                .on_tray_icon_event(|tray, event| {
                    // tray-icon owns the status-button hit target on macOS;
                    // route its native release event to the AppKit popover.
                    if matches!(
                        event,
                        TrayIconEvent::Click {
                            button: MouseButton::Left,
                            button_state: MouseButtonState::Up,
                            ..
                        }
                    ) {
                        toggle_native_popover_from_tray(tray.app_handle().clone());
                    }
                })
                .build(app)?;
            let app_handle = app.handle().clone();
            let controller = tray.with_inner_tray_icon(move |inner| {
                let mtm = MainThreadMarker::new()
                    .expect("Tauri must invoke with_inner_tray_icon on the main thread");
                let status_item = inner
                    .ns_status_item()
                    .expect("the macOS tray icon must expose an NSStatusItem");

                MainThreadBound::new(
                    NativePopoverController::attach(
                        status_item,
                        app_handle,
                        backend_paths,
                        runtime_paths,
                        service_client,
                        mtm,
                    ),
                    mtm,
                )
            })?;

            assert!(app.manage(NativeShellState(Mutex::new(Some(NativeShell {
                controller,
                tray,
            })))));
            Ok(())
        })
        .build(tauri::generate_context!())
        .map_err(|error| format!("failed to build the native menu-bar application: {error}"))?;

    app.run(|app_handle, event| {
        if let RunEvent::ExitRequested { api, .. } = event {
            dispatch_native_shell_lifecycle(
                NativeShellLifecycleEvent::ExitRequested,
                || {},
                || handle_exit_requested(app_handle, &api),
            );
        }
    });
    tracing::info!(target: "loxa_app", event = "desktop_shutdown");
    if diagnostics
        .map(loxa_diagnostics::Diagnostics::finish)
        .is_some_and(|health| !health.is_healthy())
    {
        eprintln!("Some Loxa desktop diagnostics could not be retained");
    }
    Ok(())
}

fn handle_exit_requested(app_handle: &AppHandle, api: &ExitRequestApi) {
    let exit_state = app_handle.state::<NativeExitState>();
    match exit_state.claim() {
        NativeExitAdmission::Allow => return,
        NativeExitAdmission::Prevent => {
            api.prevent_exit();
            return;
        }
        NativeExitAdmission::Proceed => {}
    }
    let shell = {
        let state = app_handle.state::<NativeShellState>();
        let mut state = state
            .0
            .lock()
            .expect("native shell state mutex must not be poisoned");
        state.take()
    };

    let Some(mut shell) = shell else {
        exit_state.reset();
        api.prevent_exit();
        return;
    };
    let mtm =
        MainThreadMarker::new().expect("Tauri must deliver native shell exit on the main thread");
    let Some(resources) = shell.controller.get_mut(mtm).prepare_exit(mtm) else {
        restore_native_shell(app_handle, shell);
        exit_state.reset();
        api.prevent_exit();
        return;
    };
    let preferences = app_handle.state::<NativePreferencesState>().0.clone();
    let attempt = NativeExitAttempt {
        shell,
        resources,
        preferences: preferences.clone(),
    };
    let preferences_exit = match preferences.begin_drain() {
        PreferencesExit::Failed => preferences.retry_for_exit(),
        state => Ok(state),
    };
    match preferences_exit {
        Ok(PreferencesExit::Drained) => complete_native_exit_request(app_handle, attempt, api),
        Ok(PreferencesExit::Pending) => {
            api.prevent_exit();
            defer_native_exit(app_handle, attempt);
        }
        Ok(PreferencesExit::Failed) | Err(_) => {
            restore_native_exit_attempt(app_handle, attempt);
            api.prevent_exit();
        }
    }
}

fn complete_native_exit_request(
    app_handle: &AppHandle,
    attempt: NativeExitAttempt,
    api: &ExitRequestApi,
) {
    match finish_native_exit(app_handle, attempt) {
        Ok(()) => app_handle.state::<NativeExitState>().complete(),
        Err(attempt) => {
            restore_native_exit_attempt(app_handle, attempt);
            api.prevent_exit();
        }
    }
}

fn finish_native_exit(
    app_handle: &AppHandle,
    attempt: NativeExitAttempt,
) -> Result<(), NativeExitAttempt> {
    run_exit_transaction(
        attempt,
        |attempt| attempt.preferences.finish_ready_drain().map_err(|_| ()),
        |attempt| attempt.resources.shutdown_runtime(),
        |attempt| attempt.resources.shutdown_backend(),
        |attempt| {
            attempt.preferences.clear_completion_notifier();
            let removed = app_handle.remove_tray_by_id("loxa");
            drop(removed);
            attempt.shell.teardown();
        },
    )
}

fn schedule_preference_completion(owner: WeakPreferencesOwner) {
    DispatchQueue::main().exec_async(move || finish_preference_completion(owner));
}

fn finish_preference_completion(owner: WeakPreferencesOwner) {
    let Some(owner) = owner.upgrade() else {
        return;
    };
    if owner.finish_notified_write() {
        return;
    }
    let owner = owner.downgrade();
    let retry_at = DispatchTime::try_from(std::time::Duration::from_millis(1))
        .expect("one millisecond must fit in dispatch time");
    let _ = DispatchQueue::main().after(retry_at, move || {
        finish_preference_completion(owner);
    });
}

fn defer_native_exit(app_handle: &AppHandle, attempt: NativeExitAttempt) {
    let preferences = attempt.preferences.clone();
    let exit_state = app_handle.state::<NativeExitState>();
    exit_state.wait_with(attempt);
    let callback_app = app_handle.clone();
    let spawned = std::thread::Builder::new()
        .name("loxa-desktop-exit-preferences".into())
        .spawn(move || {
            let result = preferences.resolve_exit_blocking();
            let scheduler = callback_app.clone();
            let _ = scheduler.run_on_main_thread(move || {
                finish_deferred_native_exit(&callback_app, result);
            });
        });
    if spawned.is_err() {
        tracing::warn!(
            target: "loxa_app",
            event = "desktop_preference_exit_observer_failed"
        );
        if let Some(attempt) = exit_state.take_waiting() {
            restore_native_exit_attempt(app_handle, attempt);
        }
    }
}

fn finish_deferred_native_exit(
    app_handle: &AppHandle,
    preferences_result: Result<(), crate::preferences::PreferenceError>,
) {
    let exit_state = app_handle.state::<NativeExitState>();
    let Some(attempt) = exit_state.take_waiting() else {
        return;
    };
    if preferences_result.is_err() {
        tracing::warn!(
            target: "loxa_app",
            event = "desktop_preference_flush_failed"
        );
        restore_native_exit_attempt(app_handle, attempt);
        return;
    }
    match finish_native_exit(app_handle, attempt) {
        Ok(()) => {
            exit_state.complete();
            app_handle.exit(0);
        }
        Err(attempt) => restore_native_exit_attempt(app_handle, attempt),
    }
}

fn restore_native_exit_attempt(app_handle: &AppHandle, attempt: NativeExitAttempt) {
    let mtm =
        MainThreadMarker::new().expect("Tauri must restore native exit state on the main thread");
    let NativeExitAttempt {
        mut shell,
        resources,
        preferences: _,
    } = attempt;
    shell.controller.get_mut(mtm).restore_exit(resources, mtm);
    restore_native_shell(app_handle, shell);
    app_handle.state::<NativeExitState>().reset();
}

fn restore_native_shell(app_handle: &AppHandle, shell: NativeShell) {
    let state = app_handle.state::<NativeShellState>();
    let mut state = state
        .0
        .lock()
        .expect("native shell state mutex must not be poisoned");
    debug_assert!(state.is_none());
    *state = Some(shell);
}

fn toggle_native_popover_from_tray(app_handle: AppHandle) {
    let main_thread_handle = app_handle.clone();
    let _ = main_thread_handle.run_on_main_thread(move || {
        let mtm = MainThreadMarker::new()
            .expect("Tauri must toggle the native popover on the main thread");
        let Some(state) = app_handle.try_state::<NativeShellState>() else {
            return;
        };
        let shell = state
            .0
            .lock()
            .expect("native shell state mutex must not be poisoned");
        if let Some(shell) = shell.as_ref() {
            shell.controller.get(mtm).toggle(mtm);
        }
    });
}

#[cfg(test)]
#[path = "menu/presentation_tests.rs"]
mod presentation_tests;

#[cfg(test)]
#[path = "app_runtime_acceptance_tests.rs"]
mod runtime_acceptance_tests;

#[cfg(test)]
mod lifecycle_tests {
    use std::cell::{Cell, RefCell};
    use std::sync::Mutex;

    use super::{
        dispatch_native_shell_lifecycle, run_exit_transaction, NativeExitAdmission,
        NativeExitPhase, NativeExitState, NativeShellLifecycleEvent,
    };
    use crate::menu::api_runtime::idle_controller_for_exit_test;
    use crate::menu::macos::{exit_resources_api, exit_resources_for_test};
    use crate::menu::observation::backend_client_panicking_on_shutdown;
    #[test]
    fn quit_requests_exit_without_synchronous_native_teardown() {
        let events = RefCell::new(Vec::new());

        dispatch_native_shell_lifecycle(
            NativeShellLifecycleEvent::QuitRequested,
            || events.borrow_mut().push("request exit"),
            || events.borrow_mut().push("teardown"),
        );

        assert_eq!(events.into_inner(), ["request exit"]);

        let events = RefCell::new(Vec::new());
        dispatch_native_shell_lifecycle(
            NativeShellLifecycleEvent::ExitRequested,
            || events.borrow_mut().push("request exit"),
            || events.borrow_mut().push("teardown"),
        );
        assert_eq!(events.into_inner(), ["teardown"]);
    }

    #[test]
    fn exit_admission_serializes_requests_and_allows_completed_reentry() {
        let state = NativeExitState::new();
        assert_eq!(state.claim(), NativeExitAdmission::Proceed);
        assert_eq!(state.claim(), NativeExitAdmission::Prevent);

        state.reset();
        assert_eq!(state.claim(), NativeExitAdmission::Proceed);
        {
            let mut inner = state.0.lock().unwrap();
            inner.phase = NativeExitPhase::Waiting;
        }
        assert_eq!(state.claim(), NativeExitAdmission::Prevent);

        {
            let mut inner = state.0.lock().unwrap();
            inner.phase = NativeExitPhase::Preparing;
        }
        state.complete();
        assert_eq!(state.claim(), NativeExitAdmission::Allow);
        assert_eq!(state.claim(), NativeExitAdmission::Allow);
    }

    #[derive(Debug, Eq, PartialEq)]
    struct ExitFixture {
        shell_id: u8,
        preference_id: u8,
        runtime_id: u8,
        backend_id: u8,
    }

    #[test]
    fn exit_transaction_joins_in_order_with_no_mutex_or_refcell_borrow_held() {
        let events = RefCell::new(Vec::new());
        let shell_mutex = Mutex::new(());
        let native_state = RefCell::new(());

        let result = run_exit_transaction(
            ExitFixture {
                shell_id: 1,
                preference_id: 4,
                runtime_id: 2,
                backend_id: 3,
            },
            |fixture| {
                assert_eq!(fixture.preference_id, 4);
                assert!(shell_mutex.try_lock().is_ok());
                assert!(native_state.try_borrow_mut().is_ok());
                events.borrow_mut().push("preferences flush");
                Ok(())
            },
            |fixture| {
                assert_eq!(fixture.runtime_id, 2);
                assert!(shell_mutex.try_lock().is_ok());
                assert!(native_state.try_borrow_mut().is_ok());
                events.borrow_mut().push("runtime join");
                Ok(())
            },
            |fixture| {
                assert_eq!(fixture.backend_id, 3);
                assert!(shell_mutex.try_lock().is_ok());
                assert!(native_state.try_borrow_mut().is_ok());
                events.borrow_mut().push("backend join");
                Ok(())
            },
            |fixture| {
                assert_eq!(fixture.shell_id, 1);
                events.borrow_mut().push("remove tray");
            },
        );

        assert_eq!(result, Ok(()));
        assert_eq!(
            events.into_inner(),
            [
                "preferences flush",
                "runtime join",
                "backend join",
                "remove tray"
            ]
        );
    }

    #[test]
    fn cleanup_failure_restores_exact_resources_blocks_teardown_and_retries() {
        let events = RefCell::new(Vec::new());
        let fixture = ExitFixture {
            shell_id: 7,
            preference_id: 6,
            runtime_id: 8,
            backend_id: 9,
        };
        let failed = run_exit_transaction(
            fixture,
            |_| {
                events.borrow_mut().push("preferences flush");
                Ok(())
            },
            |_| {
                events.borrow_mut().push("runtime failed");
                Err(())
            },
            |_| panic!("backend must not join after runtime cleanup failure"),
            |_| panic!("tray must not be removed after runtime cleanup failure"),
        )
        .expect_err("runtime cleanup failure must block exit");
        assert_eq!(
            failed,
            ExitFixture {
                shell_id: 7,
                preference_id: 6,
                runtime_id: 8,
                backend_id: 9,
            }
        );

        let retry = run_exit_transaction(
            failed,
            |_| {
                events.borrow_mut().push("preferences retry");
                Ok(())
            },
            |_| {
                events.borrow_mut().push("runtime retry");
                Ok(())
            },
            |_| {
                events.borrow_mut().push("backend retry");
                Ok(())
            },
            |_| events.borrow_mut().push("remove tray"),
        );
        assert_eq!(retry, Ok(()));
        assert_eq!(
            events.into_inner(),
            [
                "preferences flush",
                "runtime failed",
                "preferences retry",
                "runtime retry",
                "backend retry",
                "remove tray"
            ]
        );
    }

    #[test]
    fn preference_failure_retains_every_exit_resource_and_stops_cleanup() {
        let events = RefCell::new(Vec::new());
        let fixture = ExitFixture {
            shell_id: 11,
            preference_id: 12,
            runtime_id: 13,
            backend_id: 14,
        };
        let failed = run_exit_transaction(
            fixture,
            |_| {
                events.borrow_mut().push("preferences failed");
                Err(())
            },
            |_| panic!("runtime must stay retained after preference failure"),
            |_| panic!("backend must stay retained after preference failure"),
            |_| panic!("shell must stay retained after preference failure"),
        )
        .expect_err("preference failure must block exit");
        assert_eq!(
            failed,
            ExitFixture {
                shell_id: 11,
                preference_id: 12,
                runtime_id: 13,
                backend_id: 14,
            }
        );
        assert_eq!(events.borrow().as_slice(), ["preferences failed"]);

        let retried = run_exit_transaction(
            failed,
            |_| {
                events.borrow_mut().push("preferences retry");
                Ok(())
            },
            |_| {
                events.borrow_mut().push("runtime shutdown");
                Ok(())
            },
            |_| {
                events.borrow_mut().push("backend shutdown");
                Ok(())
            },
            |_| events.borrow_mut().push("shell teardown"),
        );
        assert_eq!(retried, Ok(()));
        assert_eq!(
            events.into_inner(),
            [
                "preferences failed",
                "preferences retry",
                "runtime shutdown",
                "backend shutdown",
                "shell teardown"
            ]
        );
    }

    #[test]
    fn backend_join_failure_is_unavailable_then_an_idempotent_retry_can_exit() {
        let prevented = Cell::new(0);
        let tray_removals = Cell::new(0);
        let resources = exit_resources_for_test(
            idle_controller_for_exit_test(),
            backend_client_panicking_on_shutdown(),
        );
        let failed = run_exit_transaction(
            resources,
            |_| Ok(()),
            |resources| resources.shutdown_runtime(),
            |resources| resources.shutdown_backend(),
            |_| tray_removals.set(tray_removals.get() + 1),
        )
        .expect_err("backend join failure must prevent exit");
        prevented.set(prevented.get() + 1);

        let api = exit_resources_api(&failed);
        assert_eq!(api.status_label(), "API unavailable · Quit and reopen Loxa");
        let start = api.primary_action("demo");
        assert_eq!(start.title(), "Start API");
        assert!(!start.is_enabled());
        assert_eq!(tray_removals.get(), 0);

        assert!(run_exit_transaction(
            failed,
            |_| Ok(()),
            |resources| resources.shutdown_runtime(),
            |resources| resources.shutdown_backend(),
            |_| tray_removals.set(tray_removals.get() + 1),
        )
        .is_ok());
        assert_eq!(prevented.get(), 1);
        assert_eq!(tray_removals.get(), 1);
    }
}
