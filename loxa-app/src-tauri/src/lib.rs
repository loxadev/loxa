pub mod bootstrap;

use tauri::Manager;

#[cfg(debug_assertions)]
fn init_debug_tracing() {
    let _ = tracing_subscriber::fmt()
        .compact()
        .with_target(false)
        .without_time()
        .try_init();
}

#[cfg(not(debug_assertions))]
fn init_debug_tracing() {}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    init_debug_tracing();
    let bootstrap =
        std::sync::Arc::new(std::sync::Mutex::new(bootstrap::BootstrapState::default()));
    let app = tauri::Builder::default()
        .manage(bootstrap)
        .invoke_handler(tauri::generate_handler![
            bootstrap::bootstrap_snapshot,
            bootstrap::start_node,
            bootstrap::attach_node,
            bootstrap::stop_owned_node,
            bootstrap::read_control_token
        ])
        .on_window_event(|window, event| {
            if matches!(event, tauri::WindowEvent::Destroyed) {
                let state = window.state::<bootstrap::SharedBootstrapState>();
                bootstrap::window_closed(&state);
            }
        })
        .build(tauri::generate_context!())
        .expect("error while building tauri application");
    app.run(|app_handle, event| {
        if matches!(event, tauri::RunEvent::Exit) {
            let state = app_handle.state::<bootstrap::SharedBootstrapState>();
            let mut stderr = std::io::stderr().lock();
            bootstrap::handle_exit_event(&state, &mut stderr);
        }
    });
}
