use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(
    name = "loxa",
    version,
    about = "Download and run verified Hugging Face GGUF models"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Download and verify one single-file GGUF.
    Pull(PullArgs),
    /// List locally installed models.
    List,
    /// Run a model with llama-server in the foreground.
    Run(RunArgs),
}

#[derive(Debug, Args)]
pub struct PullArgs {
    pub repo: String,
    #[arg(long)]
    pub revision: Option<String>,
    #[arg(long = "file")]
    pub filename: Option<String>,
    #[arg(long)]
    pub quant: Option<String>,
    #[arg(long)]
    pub name: Option<String>,
}

#[derive(Debug, Args)]
pub struct RunArgs {
    pub id: String,
    #[command(flatten)]
    pub runtime: RuntimeArgs,
}

#[derive(Debug, Args)]
pub struct RuntimeArgs {
    #[arg(long)]
    pub ctx: Option<u32>,
    #[arg(long)]
    pub port: Option<u16>,
    #[arg(long, hide = true)]
    pub server: Option<PathBuf>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{CommandFactory, Parser};

    #[test]
    fn exposes_exactly_pull_list_and_run_with_optional_runtime_arguments() {
        let command = Cli::command();
        let names = command
            .get_subcommands()
            .map(|command| command.get_name().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(names, ["pull", "list", "run"]);

        let run = command
            .find_subcommand("run")
            .expect("run subcommand is present");
        let ctx = run
            .get_arguments()
            .find(|argument| argument.get_id() == "ctx")
            .expect("--ctx is present");
        let port = run
            .get_arguments()
            .find(|argument| argument.get_id() == "port")
            .expect("--port is present");
        let server = run
            .get_arguments()
            .find(|argument| argument.get_id() == "server")
            .expect("--server is present");
        assert!(ctx.get_default_values().is_empty());
        assert!(port.get_default_values().is_empty());
        assert!(server.is_hide_set());

        let cli = Cli::parse_from(["loxa", "run", "demo"]);
        match cli.command {
            Command::Run(args) => {
                assert_eq!(args.id, "demo");
                assert!(args.runtime.ctx.is_none());
                assert!(args.runtime.port.is_none());
                assert!(args.runtime.server.is_none());
            }
            _ => panic!("expected run"),
        }
    }
}
