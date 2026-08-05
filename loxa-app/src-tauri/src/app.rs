use std::sync::Mutex;

use dispatch2::MainThreadBound;
use objc2::MainThreadMarker;
use tauri::tray::{MouseButton, MouseButtonState, TrayIcon, TrayIconBuilder, TrayIconEvent};
use tauri::{AppHandle, Manager, RunEvent, Wry};

use crate::menu::macos::NativePopoverController;

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

pub(crate) fn request_native_shell_exit(app_handle: &AppHandle) {
    dispatch_native_shell_lifecycle(
        NativeShellLifecycleEvent::QuitRequested,
        || app_handle.exit(0),
        || teardown_native_shell(app_handle),
    );
}

pub(crate) fn run() {
    let app = tauri::Builder::default()
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
        if matches!(event, RunEvent::ExitRequested { .. }) {
            dispatch_native_shell_lifecycle(
                NativeShellLifecycleEvent::ExitRequested,
                || {},
                || teardown_native_shell(app_handle),
            );
        }
    });
}

fn teardown_native_shell(app_handle: &AppHandle) {
    let shell = {
        let state = app_handle.state::<NativeShellState>();
        let mut state = state
            .0
            .lock()
            .expect("native shell state mutex must not be poisoned");
        state.take()
    };

    if let Some(shell) = shell {
        shell.teardown();
    }
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
    use std::cell::RefCell;

    use super::{dispatch_native_shell_lifecycle, NativeShellLifecycleEvent};

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
}
