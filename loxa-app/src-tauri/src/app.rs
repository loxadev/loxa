use std::sync::Mutex;

use dispatch2::MainThreadBound;
use objc2::MainThreadMarker;
use tauri::tray::{MouseButton, MouseButtonState, TrayIcon, TrayIconBuilder, TrayIconEvent};
use tauri::{AppHandle, ExitRequestApi, Manager, RunEvent, Wry};

use crate::menu::macos::{NativeExitResources, NativePopoverController};

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
    shutdown_runtime: impl FnOnce(&mut Resource) -> Result<(), ()>,
    shutdown_backend: impl FnOnce(&mut Resource) -> Result<(), ()>,
    finish: impl FnOnce(Resource),
) -> Result<(), Resource> {
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

pub(crate) fn run() {
    let app = crate::native_menu::with_native_edit_menu(tauri::Builder::default())
        .setup(|app| {
            app.set_activation_policy(tauri::ActivationPolicy::Accessory);

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
                    NativePopoverController::attach(status_item, app_handle, mtm),
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
        .expect("failed to build the Loxa native menu-bar application");

    app.run(|app_handle, event| {
        if let RunEvent::ExitRequested { api, .. } = event {
            dispatch_native_shell_lifecycle(
                NativeShellLifecycleEvent::ExitRequested,
                || {},
                || handle_exit_requested(app_handle, &api),
            );
        }
    });
}

fn handle_exit_requested(app_handle: &AppHandle, api: &ExitRequestApi) {
    let shell = {
        let state = app_handle.state::<NativeShellState>();
        let mut state = state
            .0
            .lock()
            .expect("native shell state mutex must not be poisoned");
        state.take()
    };

    let Some(mut shell) = shell else {
        api.prevent_exit();
        return;
    };
    let mtm =
        MainThreadMarker::new().expect("Tauri must deliver native shell exit on the main thread");
    let Some(resources) = shell.controller.get_mut(mtm).prepare_exit(mtm) else {
        restore_native_shell(app_handle, shell);
        api.prevent_exit();
        return;
    };
    let attempt = NativeExitAttempt { shell, resources };
    let result = run_exit_transaction(
        attempt,
        |attempt| attempt.resources.shutdown_runtime(),
        |attempt| attempt.resources.shutdown_backend(),
        |attempt| {
            let removed = app_handle.remove_tray_by_id("loxa");
            drop(removed);
            attempt.shell.teardown();
        },
    );
    if let Err(mut attempt) = result {
        attempt
            .shell
            .controller
            .get_mut(mtm)
            .restore_exit(attempt.resources, mtm);
        restore_native_shell(app_handle, attempt.shell);
        api.prevent_exit();
    }
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
mod lifecycle_tests {
    use std::cell::{Cell, RefCell};
    use std::sync::Mutex;

    use super::{dispatch_native_shell_lifecycle, run_exit_transaction, NativeShellLifecycleEvent};
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

    #[derive(Debug, Eq, PartialEq)]
    struct ExitFixture {
        shell_id: u8,
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
                runtime_id: 2,
                backend_id: 3,
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
            ["runtime join", "backend join", "remove tray"]
        );
    }

    #[test]
    fn cleanup_failure_restores_exact_resources_blocks_teardown_and_retries() {
        let events = RefCell::new(Vec::new());
        let fixture = ExitFixture {
            shell_id: 7,
            runtime_id: 8,
            backend_id: 9,
        };
        let failed = run_exit_transaction(
            fixture,
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
                runtime_id: 8,
                backend_id: 9,
            }
        );

        let retry = run_exit_transaction(
            failed,
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
                "runtime failed",
                "runtime retry",
                "backend retry",
                "remove tray"
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
            |resources| resources.shutdown_runtime(),
            |resources| resources.shutdown_backend(),
            |_| tray_removals.set(tray_removals.get() + 1),
        )
        .is_ok());
        assert_eq!(prevented.get(), 1);
        assert_eq!(tray_removals.get(), 1);
    }
}
