pub mod bootstrap;
mod window_layout;

use tauri::{LogicalSize, Manager};

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

fn apply_initial_window_layout(app: &tauri::App) -> tauri::Result<()> {
    let Some(window) = app.get_webview_window("main") else {
        return Ok(());
    };

    let sizing_result = (|| -> tauri::Result<()> {
        let monitor = match window.current_monitor()? {
            Some(monitor) => Some(monitor),
            None => window.primary_monitor()?,
        };
        let Some(monitor) = monitor else {
            return Ok(());
        };

        let work_area = monitor.work_area();
        let inner_size = window.inner_size()?;
        let outer_size = window.outer_size()?;
        let frame_width = outer_size.width.saturating_sub(inner_size.width);
        let frame_height = outer_size.height.saturating_sub(inner_size.height);
        let available_inner_width = work_area.size.width.saturating_sub(frame_width);
        let available_inner_height = work_area.size.height.saturating_sub(frame_height);
        let Some(layout) = window_layout::calculate_window_layout(
            available_inner_width,
            available_inner_height,
            monitor.scale_factor(),
        ) else {
            return Ok(());
        };

        window.set_min_size(Some(LogicalSize::new(layout.min_width, layout.min_height)))?;
        window.set_max_size::<LogicalSize<f64>>(None)?;
        window.set_size(LogicalSize::new(layout.width, layout.height))?;
        window.center()?;
        Ok(())
    })();
    if let Err(error) = sizing_result {
        tracing::debug!(%error, "could not apply display-relative window layout");
    }
    window.show()
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    init_debug_tracing();
    let bootstrap =
        std::sync::Arc::new(std::sync::Mutex::new(bootstrap::BootstrapState::default()));
    let app = tauri::Builder::default()
        .manage(bootstrap)
        .setup(|app| {
            apply_initial_window_layout(app)?;
            Ok(())
        })
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
