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
    /// Search Hugging Face repositories by keyword or exact repository.
    Search(SearchArgs),
    /// Inspect one Hugging Face repository for GGUF candidates.
    #[command(override_usage = "loxa inspect <OWNER/REPO> [--revision <REVISION>]")]
    Inspect(InspectArgs),
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
pub struct SearchArgs {
    /// Keyword or exact Hugging Face repository navigation input.
    pub query: String,
}

#[derive(Debug, Args)]
pub struct InspectArgs {
    /// Hugging Face repository in owner/repo form.
    #[arg(value_name = "OWNER/REPO")]
    pub repo: String,
    /// Branch, tag, or commit to inspect.
    #[arg(long)]
    pub revision: Option<String>,
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
        assert_eq!(
            names,
            ["search", "inspect", "pull", "list", "rm", "run", "chat"]
        );

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

    #[test]
    fn search_parses_one_keyword_or_exact_repository_input() {
        for input in ["gemma", "hf://owner/repo"] {
            let cli = Cli::parse_from(["loxa", "search", input]);
            match cli.command {
                Command::Search(args) => assert_eq!(args.query, input),
                _ => panic!("expected search"),
            }
        }
    }

    #[test]
    fn search_requires_one_positional_input() {
        let error = Cli::try_parse_from(["loxa", "search"]).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn search_help_matches_frozen_command_surface_exactly() {
        let error = Cli::try_parse_from(["loxa", "search", "--help"]).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::DisplayHelp);

        assert_eq!(
            error.to_string(),
            concat!(
                "Search Hugging Face repositories by keyword or exact repository\n",
                "\n",
                "Usage: loxa search <QUERY>\n",
                "\n",
                "Arguments:\n",
                "  <QUERY>  Keyword or exact Hugging Face repository navigation input\n",
                "\n",
                "Options:\n",
                "  -h, --help  Print help\n",
            )
        );
    }

    #[test]
    fn inspect_parses_canonical_repository_and_optional_revision() {
        let cli = Cli::parse_from(["loxa", "inspect", "owner/repo", "--revision", "refs/pr/7"]);
        match cli.command {
            Command::Inspect(args) => {
                assert_eq!(args.repo, "owner/repo");
                assert_eq!(args.revision.as_deref(), Some("refs/pr/7"));
            }
            _ => panic!("expected inspect"),
        }

        let cli = Cli::parse_from(["loxa", "inspect", "owner/repo"]);
        match cli.command {
            Command::Inspect(args) => {
                assert_eq!(args.repo, "owner/repo");
                assert!(args.revision.is_none());
            }
            _ => panic!("expected inspect"),
        }
    }

    #[test]
    fn inspect_requires_one_repository() {
        let error = Cli::try_parse_from(["loxa", "inspect"]).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn inspect_help_matches_frozen_command_surface_exactly() {
        let error = Cli::try_parse_from(["loxa", "inspect", "--help"]).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::DisplayHelp);

        assert_eq!(
            error.to_string(),
            concat!(
                "Inspect one Hugging Face repository for GGUF candidates\n",
                "\n",
                "Usage: loxa inspect <OWNER/REPO> [--revision <REVISION>]\n",
                "\n",
                "Arguments:\n",
                "  <OWNER/REPO>  Hugging Face repository in owner/repo form\n",
                "\n",
                "Options:\n",
                "      --revision <REVISION>  Branch, tag, or commit to inspect\n",
                "  -h, --help                 Print help\n",
            )
        );
    }

    #[test]
    fn discovery_help_has_no_selection_transfer_or_provider_flags() {
        let mut command = Cli::command();
        let search = command
            .find_subcommand_mut("search")
            .expect("search subcommand is present");
        let argument_ids = search
            .get_arguments()
            .map(|argument| argument.get_id().as_str())
            .collect::<Vec<_>>();
        assert_eq!(argument_ids, ["query"]);

        let help = search.render_long_help().to_string();
        assert!(
            help.contains("Search Hugging Face repositories by keyword or exact repository"),
            "{help}"
        );
        assert!(
            help.contains("Keyword or exact Hugging Face repository navigation input"),
            "{help}"
        );
        for excluded in [
            "--limit",
            "--sort",
            "--filter",
            "--provider",
            "--token",
            "--json",
            "--select",
            "--inspect",
            "--pull",
            "--interactive",
        ] {
            assert!(!help.contains(excluded), "unexpected {excluded} in {help}");
        }

        let inspect = command
            .find_subcommand_mut("inspect")
            .expect("inspect subcommand is present");
        let argument_ids = inspect
            .get_arguments()
            .map(|argument| argument.get_id().as_str())
            .collect::<Vec<_>>();
        assert_eq!(argument_ids, ["repo", "revision"]);

        let help = inspect.render_long_help().to_string();
        assert!(
            help.contains("Inspect one Hugging Face repository for GGUF candidates"),
            "{help}"
        );
        assert!(
            help.contains("Hugging Face repository in owner/repo form"),
            "{help}"
        );
        assert!(help.contains("Branch, tag, or commit to inspect"), "{help}");
        for excluded in [
            "--file",
            "--quant",
            "--select",
            "--pull",
            "--json",
            "--compatibility",
            "--memory",
            "--disk",
            "--runtime",
        ] {
            assert!(!help.contains(excluded), "unexpected {excluded} in {help}");
        }
    }
}
