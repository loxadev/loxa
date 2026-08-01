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
    /// List managed models and local GGUF candidates.
    List,
    /// Remove one locally installed model.
    Rm(RmArgs),
    /// Run a model with llama-server in the foreground.
    Run(RunArgs),
    /// Chat with a model in the terminal.
    Chat(ChatArgs),
}

#[derive(Debug, Args)]
pub struct RmArgs {
    /// Model ID; omit to choose an installed model.
    pub id: Option<String>,
    /// Remove without an interactive confirmation.
    #[arg(long, short = 'y')]
    pub yes: bool,
}

#[derive(Debug, Args)]
pub struct PullArgs {
    /// Hugging Face repository in owner/repo form.
    pub repo: String,
    /// Branch, tag, or commit to resolve before downloading.
    #[arg(long)]
    pub revision: Option<String>,
    /// Exact GGUF filename to download.
    #[arg(long = "file", conflicts_with = "quant")]
    pub filename: Option<String>,
    /// Quantization to select, such as Q4_K_M.
    #[arg(long)]
    pub quant: Option<String>,
    /// Local model ID used by list, rm, run, and chat.
    #[arg(long)]
    pub name: Option<String>,
}

#[derive(Debug, Args)]
pub struct RunArgs {
    /// Model ID; omit to choose a runnable model.
    pub id: Option<String>,
    #[command(flatten)]
    pub runtime: RuntimeArgs,
}

#[derive(Debug, Args)]
pub struct ChatArgs {
    /// Model ID; omit to choose a runnable model.
    pub id: Option<String>,
    #[command(flatten)]
    pub runtime: RuntimeArgs,
}

#[derive(Debug, Args)]
pub struct RuntimeArgs {
    /// Override the context window size.
    #[arg(long)]
    pub ctx: Option<u32>,
    /// Override the automatic local server port.
    #[arg(long)]
    pub port: Option<u16>,
    #[arg(long, hide = true)]
    pub server: Option<PathBuf>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{error::ErrorKind, CommandFactory, Parser};

    #[test]
    fn exposes_pull_list_run_and_chat_with_optional_runtime_arguments() {
        let command = Cli::command();
        let names = command
            .get_subcommands()
            .map(|command| command.get_name().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(names, ["pull", "list", "rm", "run", "chat"]);

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
                assert_eq!(args.id.as_deref(), Some("demo"));
                assert!(args.runtime.ctx.is_none());
                assert!(args.runtime.port.is_none());
                assert!(args.runtime.server.is_none());
            }
            _ => panic!("expected run"),
        }

        let cli = Cli::parse_from(["loxa", "chat", "demo"]);
        match cli.command {
            Command::Chat(args) => {
                assert_eq!(args.id.as_deref(), Some("demo"));
                assert!(args.runtime.ctx.is_none());
                assert!(args.runtime.port.is_none());
                assert!(args.runtime.server.is_none());
            }
            _ => panic!("expected chat"),
        }
    }

    #[test]
    fn pull_rejects_file_and_quant_together() {
        let error = Cli::try_parse_from([
            "loxa",
            "pull",
            "owner/repo",
            "--file",
            "model.gguf",
            "--quant",
            "Q4_K_M",
        ])
        .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::ArgumentConflict);
    }

    #[test]
    fn run_and_chat_accept_an_omitted_model() {
        assert!(Cli::try_parse_from(["loxa", "chat"]).is_ok());
        assert!(Cli::try_parse_from(["loxa", "run"]).is_ok());
    }

    #[test]
    fn rm_accepts_an_optional_model_and_explicit_confirmation() {
        let cli = Cli::parse_from(["loxa", "rm", "demo", "--yes"]);
        match cli.command {
            Command::Rm(args) => {
                assert_eq!(args.id.as_deref(), Some("demo"));
                assert!(args.yes);
            }
            _ => panic!("expected rm"),
        }

        assert!(Cli::try_parse_from(["loxa", "rm"]).is_ok());
    }

    #[test]
    fn chat_help_explains_model_selection_and_runtime_overrides() {
        let help = Cli::command()
            .find_subcommand_mut("chat")
            .unwrap()
            .render_long_help()
            .to_string();

        assert!(help.contains("omit to choose a runnable model"), "{help}");
        assert!(help.contains("context window"), "{help}");
        assert!(help.contains("local server port"), "{help}");
    }
}
