pub mod api_runtime;
pub mod app;
pub mod catalog;
pub mod chat;
pub mod cli;
pub mod config;
pub mod discovery;
mod download;
pub mod huggingface;
pub mod paths;
mod process_inspection;
mod runnable;
pub mod runner;
mod runtime;
#[cfg(unix)]
pub mod runtime_bundle;
mod runtime_fingerprint;
pub mod runtime_identity;
mod safe_file;
pub mod service;
mod session;
mod ui;
mod verification;

pub fn run_from_env() -> Result<i32, String> {
    cli::dispatch::run_from_env()
}

pub fn report_error(error: &str) {
    cli::dispatch::report_error(error);
}

pub fn run(cli: cli::Cli, paths: paths::AppPaths) -> Result<i32, String> {
    cli::dispatch::run(cli, paths)
}
