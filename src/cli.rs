use clap::{error::ErrorKind, ArgGroup, Args, Parser, Subcommand};
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
    /// Download and verify one explicitly selected single-file GGUF.
    Pull(PullArgs),
    /// List managed models and local GGUF candidates.
    List,
    /// Remove one locally installed model.
    Rm(RmArgs),
    /// Discard one incomplete model transfer.
    Discard(DiscardArgs),
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
    /// Branch, tag, or commit; branches/tags resolve again when pulled.
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
pub struct DiscardArgs {
    /// Model ID whose incomplete transfer should be discarded.
    #[arg(value_name = "ID")]
    pub id: String,
    /// Discard without an interactive confirmation.
    #[arg(long)]
    pub yes: bool,
}

#[derive(Debug, Args)]
#[command(
    override_usage = "loxa pull <OWNER/REPO> (--file <FILENAME>|--quant <QUANT>) [OPTIONS]\n       loxa pull <HF.CO/OWNER/REPO:FILE-OR-QUANT> [OPTIONS]",
    group(
        ArgGroup::new("artifact_selector")
            .required(false)
            .multiple(false)
            .args(["filename", "quant"])
    )
)]
pub struct PullArgs {
    /// Canonical owner/repo with exactly one selector flag, or explicit hf.co/owner/repo:<file-or-quant>.
    #[arg(value_name = "REPOSITORY")]
    pub repo: String,
    /// Branch, tag, or commit to resolve before downloading.
    #[arg(long)]
    pub revision: Option<String>,
    /// Exact GGUF filename to download.
    #[arg(long = "file")]
    pub filename: Option<String>,
    /// Quantization to select, such as Q4_K_M.
    #[arg(long)]
    pub quant: Option<String>,
    /// Local model ID used by list, rm, run, and chat.
    #[arg(long)]
    pub name: Option<String>,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct PullInput {
    pub(crate) repo: String,
    pub(crate) revision: Option<String>,
    pub(crate) filename: Option<String>,
    pub(crate) quant: Option<String>,
    pub(crate) name: Option<String>,
}

const PULL_USAGE: &str = concat!(
    "Usage: loxa pull <OWNER/REPO> (--file <FILENAME>|--quant <QUANT>) [OPTIONS]\n",
    "       loxa pull <HF.CO/OWNER/REPO:FILE-OR-QUANT> [OPTIONS]",
);

pub(crate) fn parse_checked() -> Cli {
    let result = Cli::try_parse().and_then(|cli| {
        preflight(&cli)?;
        Ok(cli)
    });
    match result {
        Ok(cli) => cli,
        Err(error) => error.exit(),
    }
}

pub(crate) fn preflight(cli: &Cli) -> Result<(), clap::Error> {
    match &cli.command {
        Command::Pull(args) => parse_pull_input(args).map(|_| ()),
        Command::Discard(args) => crate::paths::validate_id(&args.id)
            .map_err(|_| clap::Error::raw(ErrorKind::ValueValidation, "invalid model ID")),
        _ => Ok(()),
    }
}

pub(crate) fn parse_pull_input(args: &PullArgs) -> Result<PullInput, clap::Error> {
    let (repo, embedded_selection) = pull_repository_and_selection(&args.repo)?;
    if let Some(revision) = args.revision.as_deref() {
        crate::discovery::validate_revision(revision)
            .map_err(|_| pull_error(ErrorKind::ValueValidation, "invalid Hugging Face revision"))?;
    }
    let has_flag_selector = args.filename.is_some() || args.quant.is_some();
    let embedded_selector = match embedded_selection {
        EmbeddedSelection::Legacy => None,
        EmbeddedSelection::CompactMissing if has_flag_selector => {
            return Err(pull_error(
                ErrorKind::ValueValidation,
                "compact Hugging Face reference must be hf.co/owner/repo:<file-or-quant>",
            ));
        }
        EmbeddedSelection::CompactEmpty if has_flag_selector => {
            return Err(pull_error(
                ErrorKind::ValueValidation,
                "invalid compact Hugging Face selector",
            ));
        }
        EmbeddedSelection::CompactMissing | EmbeddedSelection::CompactEmpty => None,
        EmbeddedSelection::Compact(selector) => Some(selector),
    };

    let selector_count = usize::from(embedded_selector.is_some())
        + usize::from(args.filename.is_some())
        + usize::from(args.quant.is_some());
    match selector_count {
        0 => return Err(missing_selection_error(&repo, args.revision.as_deref())),
        1 => {}
        _ => {
            return Err(pull_error(
                ErrorKind::ArgumentConflict,
                "choose exactly one GGUF selector: --file, --quant, or an explicit compact reference",
            ));
        }
    }

    let mut filename = args.filename.clone();
    let mut quant = args.quant.clone();
    if let Some(selector) = embedded_selector.as_deref() {
        let suffix = selector.as_bytes().get(selector.len().saturating_sub(5)..);
        if suffix.is_some_and(|suffix| suffix.eq_ignore_ascii_case(b".gguf")) {
            filename = Some(selector.into());
        } else {
            quant = Some(selector.into());
        }
    }
    Ok(PullInput {
        repo,
        revision: args.revision.clone(),
        filename,
        quant,
        name: args.name.clone(),
    })
}

pub(crate) fn compact_file_reference(repo: &str, filename: &str) -> Option<String> {
    let reference = format!("hf.co/{repo}:{filename}");
    let parsed = parse_pull_input(&PullArgs {
        repo: reference.clone(),
        revision: None,
        filename: None,
        quant: None,
        name: None,
    })
    .ok()?;
    (parsed.repo == repo && parsed.filename.as_deref() == Some(filename)).then_some(reference)
}

enum EmbeddedSelection {
    Legacy,
    CompactMissing,
    CompactEmpty,
    Compact(String),
}

fn pull_repository_and_selection(input: &str) -> Result<(String, EmbeddedSelection), clap::Error> {
    for host in ["hf.co/", "huggingface.co/"] {
        let Some(rest) = input.strip_prefix(host) else {
            continue;
        };
        if !rest.contains('/') {
            break;
        }
        let (owner, repo_and_selector) = rest.split_once('/').ok_or_else(|| {
            pull_error(
                ErrorKind::ValueValidation,
                "compact Hugging Face reference must be hf.co/owner/repo:<file-or-quant>",
            )
        })?;
        let (repo, selector) = repo_and_selector
            .split_once(':')
            .map_or((repo_and_selector, None), |(repo, selector)| {
                (repo, Some(selector))
            });
        let canonical =
            crate::discovery::validate_repository(&format!("{owner}/{repo}")).map_err(|_| {
                pull_error(
                    ErrorKind::ValueValidation,
                    "repository must be exactly owner/repo",
                )
            })?;
        let selection = match selector {
            None => EmbeddedSelection::CompactMissing,
            Some("") => EmbeddedSelection::CompactEmpty,
            Some(selector)
                if (1..=255).contains(&selector.len())
                    && !selector.contains(['/', '\\'])
                    && !selector
                        .chars()
                        .any(crate::huggingface::unsafe_presentation_character) =>
            {
                EmbeddedSelection::Compact(selector.into())
            }
            Some(_) => {
                return Err(pull_error(
                    ErrorKind::ValueValidation,
                    "invalid compact Hugging Face selector",
                ));
            }
        };
        return Ok((canonical, selection));
    }

    crate::discovery::normalize_legacy_pull_repository(input)
        .map(|repo| (repo, EmbeddedSelection::Legacy))
        .map_err(|_| {
            pull_error(
                ErrorKind::ValueValidation,
                "repository must be exactly owner/repo",
            )
        })
}

fn missing_selection_error(repo: &str, revision: Option<&str>) -> clap::Error {
    let revision = revision
        .map(|revision| format!(" --revision={}", shell_quote(revision)))
        .unwrap_or_default();
    pull_error(
        ErrorKind::MissingRequiredArgument,
        &format!(
            concat!(
                "no GGUF was selected for {repo}\n",
                "\n",
                "Inspect every eligible GGUF:\n",
                "  loxa inspect {repo}{revision}\n",
                "\n",
                "Then choose exactly one:\n",
                "  loxa pull {repo} --file <FILENAME>{revision}\n",
                "  loxa pull {repo} --quant <QUANT>{revision}\n",
                "  loxa pull hf.co/{repo}:<FILENAME-or-QUANT>{revision}",
            ),
            repo = repo,
            revision = revision,
        ),
    )
}

fn pull_error(kind: ErrorKind, message: &str) -> clap::Error {
    clap::Error::raw(
        kind,
        format!("{message}\n\n{PULL_USAGE}\n\nFor more information, try '--help'.\n"),
    )
}

pub(crate) fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
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
            ["search", "inspect", "pull", "list", "rm", "discard", "run", "chat"]
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
    fn pull_parser_defers_exactly_one_requirement_but_keeps_flags_mutually_exclusive() {
        assert!(Cli::try_parse_from(["loxa", "pull", "owner/repo"]).is_ok());

        let cli = Cli::parse_from(["loxa", "pull", "owner/repo", "--file", "model.gguf"]);
        match cli.command {
            Command::Pull(args) => {
                assert_eq!(args.filename.as_deref(), Some("model.gguf"));
                assert!(args.quant.is_none());
            }
            _ => panic!("expected pull"),
        }

        let cli = Cli::parse_from(["loxa", "pull", "owner/repo", "--quant", "Q4_K_M"]);
        match cli.command {
            Command::Pull(args) => {
                assert!(args.filename.is_none());
                assert_eq!(args.quant.as_deref(), Some("Q4_K_M"));
            }
            _ => panic!("expected pull"),
        }

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
    fn pull_help_matches_frozen_command_surface_exactly() {
        let error = Cli::try_parse_from(["loxa", "pull", "--help"]).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::DisplayHelp);

        assert_eq!(
            error.to_string(),
            concat!(
                "Download and verify one explicitly selected single-file GGUF\n",
                "\n",
                "Usage: loxa pull <OWNER/REPO> (--file <FILENAME>|--quant <QUANT>) [OPTIONS]\n",
                "       loxa pull <HF.CO/OWNER/REPO:FILE-OR-QUANT> [OPTIONS]\n",
                "\n",
                "Arguments:\n",
                "  <REPOSITORY>  Canonical owner/repo with exactly one selector flag, or explicit hf.co/owner/repo:<file-or-quant>\n",
                "\n",
                "Options:\n",
                "      --revision <REVISION>  Branch, tag, or commit to resolve before downloading\n",
                "      --file <FILENAME>      Exact GGUF filename to download\n",
                "      --quant <QUANT>        Quantization to select, such as Q4_K_M\n",
                "      --name <NAME>          Local model ID used by list, rm, run, and chat\n",
                "  -h, --help                 Print help\n",
            )
        );
    }

    #[test]
    fn shell_quote_protects_every_complete_argument_word() {
        for (input, expected) in [
            ("ordinary.gguf", "'ordinary.gguf'"),
            ("with space.gguf", "'with space.gguf'"),
            ("quote'model.gguf", "'quote'\"'\"'model.gguf'"),
            ("$(touch sentinel).gguf", "'$(touch sentinel).gguf'"),
            ("`touch sentinel`.gguf", "'`touch sentinel`.gguf'"),
            ("name:variant.gguf", "'name:variant.gguf'"),
            ("-leading.gguf", "'-leading.gguf'"),
        ] {
            assert_eq!(shell_quote(input), expected, "{input:?}");
        }
    }

    fn compact_input(reference: String) -> Result<PullInput, clap::Error> {
        parse_pull_input(&PullArgs {
            repo: reference,
            revision: Some("refs/pr/7".into()),
            filename: None,
            quant: None,
            name: Some("local-name".into()),
        })
    }

    #[test]
    fn compact_pull_normalizes_hosts_suffix_and_literal_selector_bytes() {
        let quant = compact_input("hf.co/Owner/Repo:Q4_K_M".into()).unwrap();
        assert_eq!(quant.repo, "Owner/Repo");
        assert_eq!(quant.revision.as_deref(), Some("refs/pr/7"));
        assert_eq!(quant.filename, None);
        assert_eq!(quant.quant.as_deref(), Some("Q4_K_M"));
        assert_eq!(quant.name.as_deref(), Some("local-name"));

        let filename =
            compact_input("huggingface.co/owner/repo:Exact:Name?#@%25.GgUf".into()).unwrap();
        assert_eq!(filename.repo, "owner/repo");
        assert_eq!(filename.filename.as_deref(), Some("Exact:Name?#@%25.GgUf"));
        assert_eq!(filename.quant, None);

        let literal = compact_input("hf.co/owner/repo:Q4:K?M#tag@user%20".into()).unwrap();
        assert_eq!(literal.filename, None);
        assert_eq!(literal.quant.as_deref(), Some("Q4:K?M#tag@user%20"));
    }

    #[test]
    fn compact_pull_enforces_selector_and_derived_reference_bounds() {
        let one = compact_input("hf.co/a/b:Q".into()).unwrap();
        assert_eq!(one.quant.as_deref(), Some("Q"));

        let selector_255 = "q".repeat(255);
        let maximum = format!(
            "huggingface.co/{}/{}:{selector_255}",
            "a".repeat(96),
            "b".repeat(96)
        );
        assert_eq!(maximum.len(), 464);
        let parsed = compact_input(maximum).unwrap();
        assert_eq!(parsed.repo.len(), 193);
        assert_eq!(parsed.quant.as_deref(), Some(selector_255.as_str()));

        for invalid in [
            "hf.co/owner/repo:".into(),
            format!("hf.co/owner/repo:{}", "q".repeat(256)),
        ] {
            assert!(compact_input(invalid).is_err());
        }
    }

    #[test]
    fn compact_pull_rejects_malformed_host_repository_and_selector_forms() {
        let invalid = [
            "hf.co/owner:Q4_K_M",
            "hf.co//repo:Q4_K_M",
            "hf.co/owner/:Q4_K_M",
            "hf.co/owner/repo/extra:Q4_K_M",
            "example.com/owner/repo:Q4_K_M",
            "https://hf.co/owner/repo:Q4_K_M",
            "hf://owner/repo:Q4_K_M",
            "hf.co/user@owner/repo:Q4_K_M",
            "hf.co/owner/re?po:Q4_K_M",
            "hf.co/owner/repo#fragment:Q4_K_M",
            "hf.co/owner/repo:bad\\selector",
            "hf.co/owner/repo:bad/selector",
            "hf.co/owner/repo:bad\nselector",
            "hf.co/owner/repo:bad\u{202e}selector",
        ];

        for reference in invalid {
            assert!(
                compact_input(reference.into()).is_err(),
                "accepted {reference:?}"
            );
        }
    }

    #[test]
    fn selectorless_host_pull_preflight_rejects_flags_before_recovery() {
        let temp = tempfile::tempdir().unwrap();
        let paths = crate::paths::AppPaths::from_values(Some(temp.path()), None).unwrap();
        let cases = [
            (
                ["loxa", "pull", "hf.co/owner/repo", "--file", "model.gguf"],
                "compact Hugging Face reference must be hf.co/owner/repo:<file-or-quant>",
            ),
            (
                [
                    "loxa",
                    "pull",
                    "huggingface.co/owner/repo",
                    "--quant",
                    "Q4_K_M",
                ],
                "compact Hugging Face reference must be hf.co/owner/repo:<file-or-quant>",
            ),
            (
                ["loxa", "pull", "hf.co/owner/repo:", "--file", "model.gguf"],
                "invalid compact Hugging Face selector",
            ),
            (
                [
                    "loxa",
                    "pull",
                    "huggingface.co/owner/repo:",
                    "--quant",
                    "Q4_K_M",
                ],
                "invalid compact Hugging Face selector",
            ),
        ];

        for (argv, expected) in cases {
            let cli = Cli::parse_from(argv);
            let Command::Pull(args) = &cli.command else {
                panic!("expected pull");
            };
            let parse_error = parse_pull_input(args).unwrap_err();
            assert_eq!(parse_error.kind(), ErrorKind::ValueValidation);
            let parse_error = parse_error.to_string();
            assert!(parse_error.contains(expected), "{parse_error}");
            assert!(
                !parse_error.contains("no GGUF was selected"),
                "{parse_error}"
            );

            let calls = std::cell::Cell::new(0);
            let run_error = crate::run_with_recovery(cli, paths.clone(), |_| {
                calls.set(calls.get() + 1);
                Err("injected recovery must not run".into())
            })
            .unwrap_err();

            assert_eq!(calls.get(), 0, "{run_error}");
            assert_eq!(run_error, parse_error);
        }
    }

    #[test]
    fn selectorless_host_pull_preflight_rejects_invalid_revision_before_guidance() {
        let error = parse_pull_input(&PullArgs {
            repo: "hf.co/owner/repo:".into(),
            revision: Some("release\u{202e}UNSAFE".into()),
            filename: None,
            quant: None,
            name: None,
        })
        .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::ValueValidation);
        let error = error.to_string();
        assert!(error.contains("invalid Hugging Face revision"), "{error}");
        assert!(!error.contains("no GGUF was selected"), "{error}");
        assert!(!error.contains("UNSAFE"), "{error}");
        assert!(!error.contains('\u{202e}'), "{error}");
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
                "      --revision <REVISION>  Branch, tag, or commit; branches/tags resolve again when pulled\n",
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
        assert!(
            help.contains("Branch, tag, or commit; branches/tags resolve again when pulled"),
            "{help}"
        );
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
