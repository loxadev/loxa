mod cli;
mod model_commands;
#[cfg(test)]
mod test_support;

fn main() -> std::process::ExitCode {
    cli::main()
}
