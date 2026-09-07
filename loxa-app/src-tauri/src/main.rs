#![deny(unsafe_op_in_unsafe_fn)]

mod app;
mod menu;
mod native_menu;

fn main() {
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
