#![deny(unsafe_op_in_unsafe_fn)]

mod app;
mod menu;
mod native_menu;

fn main() {
    // Menu-copied service commands invoke the packaged desktop executable.
    // Dispatch this public CLI path before Tauri has initialized any GUI state.
    if std::env::args_os().nth(1).as_deref() == Some(std::ffi::OsStr::new("service-dev")) {
        match loxa::run_from_env() {
            Ok(code) => std::process::exit(code),
            Err(error) => {
                loxa::report_error(&error);
                std::process::exit(1);
            }
        }
    }
    match loxa::service::run_hidden_from_env() {
        loxa::service::HiddenServiceResult::NotServiceCommand => {
            if let Err(error) = app::run() {
                eprintln!("Loxa desktop failed to start: {error}");
                std::process::exit(1);
            }
        }
        loxa::service::HiddenServiceResult::Exit(Ok(code)) => std::process::exit(code),
        loxa::service::HiddenServiceResult::Exit(Err(error)) => {
            eprintln!("Loxa background service failed: {error}");
            std::process::exit(1);
        }
    }
}
