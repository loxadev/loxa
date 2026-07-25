mod cli;
mod model_commands;
mod pi_acceptance;
#[cfg(test)]
mod test_support;

fn main() -> std::process::ExitCode {
    cli::main()
}
