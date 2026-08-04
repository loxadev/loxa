#[path = "menu/macos/mod.rs"]
mod macos_menu;

use std::sync::Mutex;

use dispatch2::MainThreadBound;
use objc2::MainThreadMarker;
use tauri::tray::{TrayIcon, TrayIconBuilder};
use tauri::{AppHandle, Manager, RunEvent, Wry};

use macos_menu::NativeMenuController;

struct NativeShell {
    controller: MainThreadBound<NativeMenuController>,
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

pub(crate) fn run() {
    let app = tauri::Builder::default()
        .setup(|app| {
            app.set_activation_policy(tauri::ActivationPolicy::Accessory);

            // The macOS tray target only forwards a left click to the status
            // item when the menu was registered through the tray builder.
            // NativeMenuController populates this exact NSMenu below.
            let menu = tauri::menu::Menu::new(app)?;
            let tray = TrayIconBuilder::with_id("loxa")
                .menu(&menu)
                .icon(tauri::include_image!("./icons/loxa-template.png"))
                .icon_as_template(true)
                .tooltip("Loxa")
                .build(app)?;
            let app_handle = app.handle().clone();
            let controller = tray.with_inner_tray_icon(move |inner| {
                let mtm = MainThreadMarker::new()
                    .expect("Tauri must invoke with_inner_tray_icon on the main thread");
                let status_item = inner
                    .ns_status_item()
                    .expect("the macOS tray icon must expose an NSStatusItem");

                MainThreadBound::new(
                    NativeMenuController::attach(status_item, app_handle, mtm),
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
            teardown_native_shell(app_handle);
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
