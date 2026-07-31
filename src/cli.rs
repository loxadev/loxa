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
    /// Chat with a model in the terminal.
    Chat(ChatArgs),
}

#[derive(Debug, Args)]
pub struct PullArgs {
    pub repo: String,
    #[arg(long)]
    pub revision: Option<String>,
    #[arg(long = "file", conflicts_with = "quant")]
    pub filename: Option<String>,
    #[arg(long)]
    pub quant: Option<String>,
    #[arg(long)]
    pub name: Option<String>,
}

#[derive(Debug, Args)]
pub struct RunArgs {
    /// Installed model ID.
    pub id: String,
    #[command(flatten)]
    pub runtime: RuntimeArgs,
}

#[derive(Debug, Args)]
pub struct ChatArgs {
    /// Model ID; omit to choose an installed model.
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
        assert_eq!(names, ["pull", "list", "run", "chat"]);

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
    fn chat_accepts_an_omitted_model_but_run_does_not() {
        assert!(Cli::try_parse_from(["loxa", "chat"]).is_ok());
        assert!(Cli::try_parse_from(["loxa", "run"]).is_err());
    }

    #[test]
    fn chat_help_explains_model_selection_and_runtime_overrides() {
        let help = Cli::command()
            .find_subcommand_mut("chat")
            .unwrap()
            .render_long_help()
            .to_string();

        assert!(help.contains("omit to choose an installed model"), "{help}");
        assert!(help.contains("context window"), "{help}");
        assert!(help.contains("local server port"), "{help}");
    }
}
