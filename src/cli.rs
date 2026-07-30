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
    #[arg(long, default_value_t = 4096)]
    pub ctx: u32,
    #[arg(long, default_value_t = 0)]
    pub port: u16,
    #[arg(long)]
    pub server: Option<PathBuf>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{CommandFactory, Parser};

    #[test]
    fn exposes_exactly_pull_list_and_run_with_pinned_defaults() {
        let names = Cli::command()
            .get_subcommands()
            .map(|command| command.get_name().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(names, ["pull", "list", "run"]);

        let cli = Cli::parse_from(["loxa", "run", "demo"]);
        match cli.command {
            Command::Run(args) => {
                assert_eq!(args.id, "demo");
                assert_eq!(args.ctx, 4096);
                assert_eq!(args.port, 0);
                assert!(args.server.is_none());
            }
            _ => panic!("expected run"),
        }
    }
}
