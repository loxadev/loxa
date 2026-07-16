#[cfg(test)]
use crate::model_commands::{
    bytes_to_gb_string, model_paths, model_status, remove_model_files, remove_user_entry,
    ModelStatus,
};
use crate::model_commands::{print_list, pull_model, remove_model, write_unknown_id};
use clap::Parser;
use loxa_core::control::auth::ControlToken;
use loxa_core::control::client::{
    ChatView, LiveControlClient, MessagePageView, MessageSummaryView, ProvenControlPeer,
    TurnStreamEvent, TurnView,
};
use loxa_core::detect::{DetectedTool, LocalToolsReport};
use loxa_core::engine::RuntimeBackendKind;
use loxa_core::hardware::HardwareReport;
use loxa_core::registry::{self, ModelEntry, REGISTRY};
#[cfg(test)]
use loxa_core::supervisor::ManagedServer;
use loxa_core::supervisor::RuntimeStateRead;
use loxa_core::supervisor::{self, SupervisorError};
use loxa_node::*;
use std::io::{self, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::process::ExitCode;
#[cfg(unix)]
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

#[cfg(unix)]
static CHAT_INTERRUPTED: AtomicBool = AtomicBool::new(false);

#[cfg(unix)]
extern "C" fn record_chat_interrupt(_signal: std::ffi::c_int) {
    CHAT_INTERRUPTED.store(true, Ordering::SeqCst);
}

#[cfg(unix)]
struct ChatInterruptGuard {
    previous: usize,
}

#[cfg(unix)]
impl ChatInterruptGuard {
    fn install() -> io::Result<Self> {
        extern "C" {
            fn signal(signal: std::ffi::c_int, handler: usize) -> usize;
        }
        CHAT_INTERRUPTED.store(false, Ordering::SeqCst);
        let previous = unsafe { signal(2, record_chat_interrupt as *const () as usize) };
        if previous == usize::MAX {
            Err(io::Error::last_os_error())
        } else {
            Ok(Self { previous })
        }
    }

    fn interrupted(&self) -> bool {
        CHAT_INTERRUPTED.load(Ordering::SeqCst)
    }
}

#[cfg(unix)]
impl Drop for ChatInterruptGuard {
    fn drop(&mut self) {
        extern "C" {
            fn signal(signal: std::ffi::c_int, handler: usize) -> usize;
        }
        let _ = unsafe { signal(2, self.previous) };
        CHAT_INTERRUPTED.store(false, Ordering::SeqCst);
    }
}

#[cfg(not(unix))]
struct ChatInterruptGuard;

#[cfg(not(unix))]
impl ChatInterruptGuard {
    fn install() -> io::Result<Self> {
        Ok(Self)
    }

    fn interrupted(&self) -> bool {
        false
    }
}

#[derive(Parser)]
#[command(name = "loxa", version, about = "Measured local AI infrastructure")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(clap::Subcommand)]
enum Command {
    Calibrate,
    Doctor,
    Pull {
        id: String,
        #[arg(long)]
        quant: Option<String>,
    },
    List,
    Rm {
        id: String,
    },
    Load {
        id: String,
    },
    Unload,
    Chat {
        #[arg(long)]
        chat: Option<String>,
        prompt: String,
    },
    Chats {
        #[command(subcommand)]
        command: ChatsCommand,
    },
    Run {
        id: String,
        #[arg(long)]
        ctx: Option<u32>,
        #[arg(long)]
        port: Option<u16>,
        #[arg(long, default_value_t = RuntimeBackendKind::LlamaCpp)]
        engine: RuntimeBackendKind,
    },
    Serve {
        #[arg(long)]
        model: Option<String>,
        #[arg(long)]
        port: Option<u16>,
        #[arg(long, default_value_t = RuntimeBackendKind::LlamaCpp)]
        engine: RuntimeBackendKind,
    },
    Ps,
    Stop {
        target: String,
    },
}

#[derive(clap::Subcommand)]
enum ChatsCommand {
    List {
        #[arg(long, default_value_t = 30)]
        limit: usize,
        #[arg(long)]
        before: Option<String>,
        #[arg(long)]
        json: bool,
    },
    Show {
        id: String,
        #[arg(long)]
        json: bool,
    },
    Rename {
        id: String,
        title: String,
    },
    Delete {
        id: String,
        #[arg(long)]
        yes: bool,
    },
    Clear {
        #[arg(long)]
        yes: bool,
    },
}

pub(crate) fn main() -> ExitCode {
    let cli = Cli::parse();
    let paths = NodePaths::detect();
    let diagnostics = matches!(&cli.command, Command::Serve { .. })
        .then(|| install_daemon_diagnostics(&paths.logs_dir));
    let exit_code = run_with_paths_and_diagnostics_health(
        cli,
        &paths,
        &mut io::stdout(),
        &mut io::stderr(),
        diagnostics.as_ref().map(|bootstrap| bootstrap.health()),
    );
    if diagnostics.is_some() {
        let result_class = if exit_code == ExitCode::SUCCESS {
            "success"
        } else {
            "failed"
        };
        emit_final_shutdown_diagnostic(result_class);
    }
    drop(diagnostics);
    exit_code
}

#[cfg_attr(not(test), allow(dead_code))]
fn run<W: Write, E: Write>(cli: Cli, mut stdout: W, mut stderr: E) -> ExitCode {
    let paths = NodePaths::detect();
    run_with_paths(cli, &paths, &mut stdout, &mut stderr)
}

#[cfg_attr(not(test), allow(dead_code))]
fn run_with_paths<W: Write, E: Write>(
    cli: Cli,
    paths: &NodePaths,
    mut stdout: W,
    mut stderr: E,
) -> ExitCode {
    run_with_paths_and_diagnostics_health(cli, paths, &mut stdout, &mut stderr, None)
}

fn run_with_paths_and_diagnostics_health<W: Write, E: Write>(
    cli: Cli,
    paths: &NodePaths,
    mut stdout: W,
    mut stderr: E,
    diagnostics_health: Option<loxa_core::diagnostics::DiagnosticsHealth>,
) -> ExitCode {
    if let Err(error) = validate_cli_contract(&cli) {
        return finish_cli_result(Err(error), &mut stderr);
    }
    let result = (|| -> io::Result<ExitCode> {
        match cli.command {
            Command::Calibrate => run_calibration(&mut stdout),
            Command::Doctor => print_doctor(&mut stdout),
            Command::Pull { id, quant } => match live_control(paths)? {
                Some(client) => live_pull(&client, &id, quant.as_deref(), &mut stdout),
                None => offline_pull_with(paths, || {
                    pull_model(&id, quant.as_deref(), &mut stdout, &mut stderr)
                }),
            },
            Command::List => match live_control(paths)? {
                Some(client) => live_list(&client, &mut stdout),
                None => print_list(&mut stdout),
            },
            Command::Rm { id } => match live_control(paths)? {
                Some(_) => {
                    writeln!(
                        stderr,
                        "cannot remove {id} while a managed node is running; stop the node first (no authenticated remove API is approved)"
                    )?;
                    Ok(ExitCode::from(1))
                }
                None => offline_rm_with(paths, || remove_model(&id, &mut stdout, &mut stderr)),
            },
            Command::Load { id } => match live_control(paths)? {
                Some(client) => live_operation(&client, client.load(&id), "load", &mut stdout),
                None => {
                    writeln!(
                        stderr,
                        "no managed node is running; start one with `loxa serve`, then run `loxa load {id}`"
                    )?;
                    Ok(ExitCode::from(1))
                }
            },
            Command::Unload => match live_control(paths)? {
                Some(client) => live_operation(&client, client.unload(), "unload", &mut stdout),
                None => {
                    writeln!(stderr, "no managed node is running; nothing to unload")?;
                    Ok(ExitCode::from(1))
                }
            },
            Command::Chat { chat, prompt } => {
                let client = require_live_control(paths, "chat")?;
                live_chat(&client, chat.as_deref(), &prompt, &mut stdout, &mut stderr)
            }
            Command::Chats { command } => {
                let client = require_live_control(paths, "chat history")?;
                live_chats(&client, command, &mut stdout)
            }
            Command::Run {
                id,
                ctx,
                port,
                engine,
            } => run_model_cli(&id, ctx, port, engine, paths, &mut stdout, &mut stderr),
            Command::Serve {
                model,
                port,
                engine,
            } => serve_node_cli(
                model.as_deref(),
                port,
                engine,
                paths,
                &mut stdout,
                &mut stderr,
                diagnostics_health.as_ref(),
            ),
            Command::Ps => render_managed_servers(managed_servers(paths), &mut stdout),
            Command::Stop { target } => render_stop_outcome(
                &target,
                stop_managed_servers(StopRequest { target: &target }, paths),
                &mut stdout,
                &mut stderr,
            ),
        }
    })();

    finish_cli_result(result, &mut stderr)
}

fn require_live_control(paths: &NodePaths, action: &str) -> io::Result<LiveControlClient> {
    live_control(paths)?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotConnected,
            format!(
                "no managed node is running; start one with `loxa serve` before using {action}"
            ),
        )
    })
}

fn live_chat<W: Write, E: Write>(
    client: &LiveControlClient,
    requested_chat: Option<&str>,
    prompt: &str,
    stdout: &mut W,
    stderr: &mut E,
) -> io::Result<ExitCode> {
    let chat = match requested_chat {
        Some(id) => client.chat(id),
        None => client.create_chat(),
    }
    .map_err(io::Error::other)?;
    writeln!(stderr, "chat: {}", chat.id)?;
    let interrupt = ChatInterruptGuard::install()?;
    let streamed = client.stream_turn_with_cancel(
        &chat.id,
        prompt,
        || interrupt.interrupted(),
        |event| {
            if interrupt.interrupted() {
                return Err(loxa_core::control::client::ClientError::OperationCancelled);
            }
            match &event {
                TurnStreamEvent::Started { omitted_turns, .. } if *omitted_turns > 0 => {
                    writeln!(
                    stderr,
                    "note: {omitted_turns} older completed turns were omitted from model context"
                )
                    .map_err(|error| {
                        loxa_core::control::client::ClientError::Transport(error.to_string())
                    })?;
                }
                TurnStreamEvent::Delta { content, .. } => {
                    write!(stdout, "{content}")
                        .and_then(|_| stdout.flush())
                        .map_err(|error| {
                            loxa_core::control::client::ClientError::Transport(error.to_string())
                        })?;
                }
                _ => {}
            }
            Ok(())
        },
    );
    if interrupt.interrupted() && streamed.is_err() {
        writeln!(stdout)?;
        writeln!(stderr, "chat turn cancelled")?;
        return Ok(ExitCode::from(130));
    }
    let terminal = streamed.map_err(io::Error::other)?;
    writeln!(stdout)?;
    match terminal {
        TurnStreamEvent::Terminal { state, .. } if state == "completed" => Ok(ExitCode::SUCCESS),
        TurnStreamEvent::Terminal { state, .. } if state == "cancelled" => {
            writeln!(stderr, "chat turn cancelled")?;
            Ok(ExitCode::from(130))
        }
        TurnStreamEvent::Terminal { error_code, .. } => Err(io::Error::other(format!(
            "chat turn failed{}",
            error_code
                .as_deref()
                .map(|code| format!(" ({code})"))
                .unwrap_or_default()
        ))),
        _ => Err(io::Error::other(
            "chat stream ended without a terminal event",
        )),
    }
}

fn live_chats<W: Write>(
    client: &LiveControlClient,
    command: ChatsCommand,
    stdout: &mut W,
) -> io::Result<ExitCode> {
    match command {
        ChatsCommand::List {
            limit,
            before,
            json,
        } => {
            let page = client
                .chats(limit, before.as_deref())
                .map_err(io::Error::other)?;
            if json {
                write_chat_list_json(stdout, &page.chats, page.next_before.as_deref())?;
            } else {
                writeln!(
                    stdout,
                    "id                                updated        title"
                )?;
                for chat in page.chats {
                    writeln!(
                        stdout,
                        "{}  {:>13}  {}",
                        chat.id, chat.updated_at_ms, chat.title
                    )?;
                }
                if let Some(cursor) = page.next_before {
                    writeln!(stdout, "more chats available; next cursor: {cursor}")?;
                }
            }
        }
        ChatsCommand::Show { id, json } => {
            let chat = client.chat(&id).map_err(io::Error::other)?;
            let turns = fetch_all_turns(client, &id)?;
            let messages = fetch_turn_messages(client, &id, &turns)?;
            if json {
                write_chat_show_json(stdout, &chat, &turns, &messages)?;
            } else {
                writeln!(stdout, "{} ({})", chat.title, chat.id)?;
                if turns.len() != messages.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "turn and message history groups do not match",
                    ));
                }
                for (turn_index, turn) in turns.iter().enumerate() {
                    let turn_messages = &messages[turn_index];
                    writeln!(stdout, "\nturn {} [{}]", turn.ordinal, turn.state)?;
                    for (summary, content) in turn_messages {
                        writeln!(stdout, "\n{}:\n{}", summary.role, content)?;
                    }
                }
            }
        }
        ChatsCommand::Rename { id, title } => {
            let chat = client.rename_chat(&id, &title).map_err(io::Error::other)?;
            writeln!(stdout, "renamed {} to {}", chat.id, chat.title)?;
        }
        ChatsCommand::Delete { id, yes } => {
            require_confirmation(yes, "delete").map_err(io::Error::other)?;
            client.delete_chat(&id).map_err(io::Error::other)?;
            writeln!(stdout, "deleted chat {id}")?;
        }
        ChatsCommand::Clear { yes } => {
            require_confirmation(yes, "clear").map_err(io::Error::other)?;
            let deleted = client.clear_chats().map_err(io::Error::other)?;
            writeln!(stdout, "deleted {deleted} chats")?;
        }
    }
    Ok(ExitCode::SUCCESS)
}

fn require_confirmation(yes: bool, action: &str) -> Result<(), &'static str> {
    if yes {
        Ok(())
    } else if action == "delete" {
        Err("refusing to delete chat history without --yes")
    } else {
        Err("refusing to clear chat history without --yes")
    }
}

type CliMessages = Vec<Vec<(MessageSummaryView, String)>>;

fn fetch_all_turns(client: &LiveControlClient, chat_id: &str) -> io::Result<Vec<TurnView>> {
    let mut turns = Vec::new();
    let mut after = None;
    loop {
        let page = client
            .turns(chat_id, 100, after.as_deref())
            .map_err(io::Error::other)?;
        append_turn_page(chat_id, &mut turns, page.turns)?;
        match page.next_after {
            Some(next) if after.as_deref() != Some(next.as_str()) => after = Some(next),
            Some(_) => return Err(io::Error::other("history pagination cursor repeated")),
            None => return Ok(turns),
        }
    }
}

fn append_turn_page(
    chat_id: &str,
    turns: &mut Vec<TurnView>,
    page: Vec<TurnView>,
) -> io::Result<()> {
    for turn in page {
        if turn.chat_id != chat_id
            || turns
                .last()
                .is_some_and(|previous| turn.ordinal <= previous.ordinal)
            || turns.iter().any(|previous| previous.id == turn.id)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "turn history is not strictly ordered",
            ));
        }
        turns.push(turn);
    }
    Ok(())
}

fn fetch_turn_messages(
    client: &LiveControlClient,
    chat_id: &str,
    turns: &[TurnView],
) -> io::Result<CliMessages> {
    let mut output = Vec::with_capacity(turns.len());
    for turn in turns {
        let summaries = client
            .message_summaries(chat_id, &turn.id)
            .map_err(io::Error::other)?;
        if summaries.messages.len() != 2
            || summaries.messages[0].turn_id != turn.id
            || summaries.messages[0].role != "user"
            || summaries.messages[1].turn_id != turn.id
            || summaries.messages[1].role != "assistant"
            || summaries.messages[0].id == summaries.messages[1].id
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "turn message history is incomplete or inconsistent",
            ));
        }
        let mut messages = Vec::with_capacity(summaries.messages.len());
        for summary in summaries.messages {
            let mut pages = Vec::new();
            let mut segment = 0;
            loop {
                let page = client
                    .message_page(chat_id, &turn.id, &summary.id, segment)
                    .map_err(io::Error::other)?;
                let next = page.next_segment;
                pages.push(page);
                match next {
                    Some(next) if next > segment => segment = next,
                    Some(_) => return Err(io::Error::other("message segment cursor repeated")),
                    None => break,
                }
            }
            let content = join_message_pages(&summary, &pages)?;
            messages.push((summary, content));
        }
        output.push(messages);
    }
    Ok(output)
}

fn join_message_pages(
    summary: &MessageSummaryView,
    pages: &[MessagePageView],
) -> io::Result<String> {
    if pages.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "message history has no segments",
        ));
    }
    let mut content = String::new();
    let mut expected = 0_u32;
    let expected_count = pages.first().map(|page| page.segment_count).unwrap_or(0);
    if expected_count == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "message history declared zero segments",
        ));
    }
    for page in pages {
        if page.message_id != summary.id
            || page.turn_id != summary.turn_id
            || page.role != summary.role
            || page.segment_count != expected_count
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "message history metadata changed between segments",
            ));
        }
        for segment in &page.segments {
            if segment.message_id != summary.id
                || segment.turn_id != summary.turn_id
                || segment.role != summary.role
                || segment.segment_index != expected
                || segment.segment_count != expected_count
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "message history segments are out of order",
                ));
            }
            content.push_str(&segment.content);
            expected += 1;
        }
    }
    if expected != expected_count {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "message history is missing segments",
        ));
    }
    if content.len() != summary.content_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "message history byte count does not match its summary",
        ));
    }
    Ok(content)
}

fn write_json_string<W: Write>(writer: &mut W, value: &str) -> io::Result<()> {
    writer.write_all(b"\"")?;
    for character in value.chars() {
        match character {
            '"' => writer.write_all(b"\\\"")?,
            '\\' => writer.write_all(b"\\\\")?,
            '\n' => writer.write_all(b"\\n")?,
            '\r' => writer.write_all(b"\\r")?,
            '\t' => writer.write_all(b"\\t")?,
            character if character.is_control() => write!(writer, "\\u{:04x}", character as u32)?,
            character => write!(writer, "{character}")?,
        }
    }
    writer.write_all(b"\"")
}

fn write_chat_json<W: Write>(writer: &mut W, chat: &ChatView) -> io::Result<()> {
    writer.write_all(b"{\"id\":")?;
    write_json_string(writer, &chat.id)?;
    writer.write_all(b",\"title\":")?;
    write_json_string(writer, &chat.title)?;
    write!(
        writer,
        ",\"created_at_ms\":{},\"updated_at_ms\":{}}}",
        chat.created_at_ms, chat.updated_at_ms
    )
}

fn write_chat_list_json<W: Write>(
    writer: &mut W,
    chats: &[ChatView],
    next_before: Option<&str>,
) -> io::Result<()> {
    writer.write_all(b"{\"chats\":[")?;
    for (index, chat) in chats.iter().enumerate() {
        if index > 0 {
            writer.write_all(b",")?;
        }
        write_chat_json(writer, chat)?;
    }
    writer.write_all(b"],\"next_before\":")?;
    match next_before {
        Some(cursor) => write_json_string(writer, cursor)?,
        None => writer.write_all(b"null")?,
    }
    writer.write_all(b"}\n")
}

fn write_chat_show_json<W: Write>(
    writer: &mut W,
    chat: &ChatView,
    turns: &[TurnView],
    messages: &CliMessages,
) -> io::Result<()> {
    if turns.len() != messages.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "turn and message history groups do not match",
        ));
    }
    writer.write_all(b"{\"chat\":")?;
    write_chat_json(writer, chat)?;
    writer.write_all(b",\"turns\":[")?;
    for (turn_index, turn) in turns.iter().enumerate() {
        let turn_messages = &messages[turn_index];
        if turn_index > 0 {
            writer.write_all(b",")?;
        }
        writer.write_all(b"{\"id\":")?;
        write_json_string(writer, &turn.id)?;
        write!(writer, ",\"ordinal\":{},\"state\":", turn.ordinal)?;
        write_json_string(writer, &turn.state)?;
        writer.write_all(b",\"provenance\":{\"model_alias\":")?;
        write_json_string(writer, &turn.provenance.model_alias)?;
        writer.write_all(b",\"recipe_id\":")?;
        write_json_string(writer, &turn.provenance.recipe_id)?;
        writer.write_all(b",\"engine_name\":")?;
        match turn.provenance.engine_name.as_deref() {
            Some(value) => write_json_string(writer, value)?,
            None => writer.write_all(b"null")?,
        }
        writer.write_all(b",\"engine_version\":")?;
        match turn.provenance.engine_version.as_deref() {
            Some(value) => write_json_string(writer, value)?,
            None => writer.write_all(b"null")?,
        }
        writer.write_all(b"},\"error_code\":")?;
        match turn.error_code.as_deref() {
            Some(value) => write_json_string(writer, value)?,
            None => writer.write_all(b"null")?,
        }
        writer.write_all(b",\"messages\":[")?;
        for (message_index, (summary, content)) in turn_messages.iter().enumerate() {
            if message_index > 0 {
                writer.write_all(b",")?;
            }
            writer.write_all(b"{\"id\":")?;
            write_json_string(writer, &summary.id)?;
            writer.write_all(b",\"role\":")?;
            write_json_string(writer, &summary.role)?;
            writer.write_all(b",\"content\":")?;
            write_json_string(writer, content)?;
            writer.write_all(b"}")?;
        }
        writer.write_all(b"]}")?;
    }
    writer.write_all(b"]}\n")
}

fn offline_pull_with(
    paths: &NodePaths,
    mutation: impl FnOnce() -> io::Result<ExitCode>,
) -> io::Result<ExitCode> {
    let _admission = supervisor::admit_offline_model_mutation(&paths.state_path)
        .map_err(supervisor_error_to_io)?;
    mutation()
}

fn offline_rm_with(
    paths: &NodePaths,
    mutation: impl FnOnce() -> io::Result<ExitCode>,
) -> io::Result<ExitCode> {
    let _admission = supervisor::admit_offline_model_mutation(&paths.state_path)
        .map_err(supervisor_error_to_io)?;
    mutation()
}

fn live_control(paths: &NodePaths) -> io::Result<Option<LiveControlClient>> {
    let runs =
        match supervisor::read_runtime_state(&paths.state_path).map_err(supervisor_error_to_io)? {
            RuntimeStateRead::Missing => return Ok(None),
            RuntimeStateRead::Legacy(path) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "legacy managed node state at {}; recovery required before model control",
                        path.display()
                    ),
                ));
            }
            RuntimeStateRead::Corrupt(message) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("managed node state is corrupt: {message}; recovery required"),
                ));
            }
            RuntimeStateRead::Loaded(runs) => runs,
        };
    if runs.len() > 1 {
        return Err(io::Error::other(
            "multiple managed node owners are present; recovery required before model control",
        ));
    }
    let Some(run) = runs.into_iter().next() else {
        return Ok(None);
    };
    let inspection = supervisor::inspect_managed_run(&run);
    if inspection.owner_status != supervisor::OwnerIdentityStatus::Live
        || matches!(
            inspection.status,
            supervisor::ManagedRunStatus::RecoveryRequired
        )
    {
        return Err(io::Error::other(format!(
            "managed node ownership is {:?}; recovery required before model control",
            inspection.owner_status
        )));
    }
    if matches!(inspection.status, supervisor::ManagedRunStatus::Stopping) {
        return Err(io::Error::other(
            "managed node is stopping; model control is unavailable",
        ));
    }
    if !matches!(
        inspection.status,
        supervisor::ManagedRunStatus::Unloaded | supervisor::ManagedRunStatus::Running
    ) {
        return Err(io::Error::other(format!(
            "managed node is {:?}; wait for a stable unloaded or running state",
            inspection.status
        )));
    }
    let address = live_control_address(&run)?;
    let state_dir = paths
        .state_path
        .parent()
        .ok_or_else(|| io::Error::other("managed state path has no parent"))?;
    let loxa_dir = if state_dir.file_name().is_some_and(|name| name == "run") {
        state_dir
            .parent()
            .ok_or_else(|| io::Error::other("managed state path has no Loxa directory"))?
    } else {
        state_dir
    };
    let token = ControlToken::load(&loxa_dir.join("control.token"))?;
    let proved = ProvenControlPeer::prove(address, token, Duration::from_millis(750))
        .map_err(io::Error::other)?;
    match supervisor::read_runtime_state(&paths.state_path).map_err(supervisor_error_to_io)? {
        RuntimeStateRead::Loaded(current)
            if current.len() == 1
                && current[0] == run
                && acceptable_live_control_run(&current[0]) => {}
        _ => {
            return Err(io::Error::other(
                "managed node state changed during peer proof; retry the command",
            ))
        }
    }
    Ok(Some(proved.into_client()))
}

fn acceptable_live_control_run(run: &supervisor::ManagedRun) -> bool {
    let inspection = supervisor::inspect_managed_run(run);
    inspection.owner_status == supervisor::OwnerIdentityStatus::Live
        && matches!(
            inspection.status,
            supervisor::ManagedRunStatus::Unloaded | supervisor::ManagedRunStatus::Running
        )
}

fn live_control_address(run: &supervisor::ManagedRun) -> io::Result<SocketAddr> {
    let port = run.control_port.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::Unsupported,
            "the managed runtime does not expose an authenticated node-control endpoint; stop it and start `loxa serve` with this version",
        )
    })?;
    Ok(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port))
}

fn live_operation<W: Write>(
    client: &LiveControlClient,
    started: Result<String, loxa_core::control::client::ClientError>,
    label: &str,
    stdout: &mut W,
) -> io::Result<ExitCode> {
    let operation_id = started.map_err(io::Error::other)?;
    writeln!(stdout, "{label} operation {operation_id} accepted")?;
    client
        .wait_terminal(&operation_id, Duration::from_secs(24 * 60 * 60))
        .map_err(io::Error::other)?;
    writeln!(stdout, "{label} completed")?;
    Ok(ExitCode::SUCCESS)
}

fn live_pull<W: Write>(
    client: &LiveControlClient,
    id: &str,
    quant: Option<&str>,
    stdout: &mut W,
) -> io::Result<ExitCode> {
    if quant.is_some() || id.starts_with("hf://") || id.matches('/').count() == 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "a running node accepts only known registry recipe IDs; stop it before resolving a Hugging Face reference or custom quantization",
        ));
    }
    live_operation(client, client.download(id), "download", stdout)
}

fn live_list<W: Write>(client: &LiveControlClient, stdout: &mut W) -> io::Result<ExitCode> {
    writeln!(stdout, "id  status  compatible  engine")?;
    for model in client.models().map_err(io::Error::other)? {
        writeln!(
            stdout,
            "{}  {}  {}  {}",
            model.id,
            live_artifact_status(&model.artifact),
            model.compatibility.compatible,
            model.engine.engine
        )?;
    }
    Ok(ExitCode::SUCCESS)
}

fn live_artifact_status(artifact: &loxa_core::model_inventory::ArtifactState) -> &'static str {
    use loxa_core::model_inventory::ArtifactState;
    match artifact {
        ArtifactState::NotDownloaded => "not_downloaded",
        ArtifactState::Partial { .. } => "partial",
        ArtifactState::Downloaded => "downloaded",
        ArtifactState::Invalid { .. } => "invalid",
    }
}

fn run_model_cli<W: Write, E: Write>(
    id: &str,
    ctx: Option<u32>,
    port: Option<u16>,
    engine: RuntimeBackendKind,
    paths: &NodePaths,
    stdout: &mut W,
    stderr: &mut E,
) -> io::Result<ExitCode> {
    if engine == RuntimeBackendKind::LlamaCpp {
        let Some(_) = registry::find(id) else {
            write_unknown_id(id, stderr)?;
            return Ok(ExitCode::from(1));
        };
    }
    let outcome = {
        let mut events = CliLifecycleSink { stdout, stderr };
        run_model(
            RunRequest {
                id,
                ctx,
                port,
                engine,
            },
            paths,
            None,
            &mut events,
        )
    };
    match outcome {
        Err(error)
            if engine == RuntimeBackendKind::LlamaCpp
                && error.kind() == io::ErrorKind::NotFound =>
        {
            writeln!(
                stderr,
                "model not downloaded for {id}; run `loxa pull {id}`"
            )?;
            Ok(ExitCode::from(1))
        }
        Ok(outcome) => Ok(exit_code_for_termination(outcome)),
        Err(error) => Err(error),
    }
}

fn serve_node_cli<W: Write, E: Write>(
    requested_model: Option<&str>,
    port: Option<u16>,
    engine: RuntimeBackendKind,
    paths: &NodePaths,
    stdout: &mut W,
    stderr: &mut E,
    diagnostics_health: Option<&loxa_core::diagnostics::DiagnosticsHealth>,
) -> io::Result<ExitCode> {
    validate_cli_serve_request(requested_model, engine, paths)?;
    let mut events = CliLifecycleSink { stdout, stderr };
    loxa_node::serve_node_with_diagnostics_health(
        requested_model,
        port,
        engine,
        paths,
        &mut events,
        diagnostics_health.cloned().unwrap_or_default(),
    )
    .map(exit_code_for_termination)
}

fn validate_cli_serve_request(
    requested_model: Option<&str>,
    engine: RuntimeBackendKind,
    paths: &NodePaths,
) -> io::Result<()> {
    if engine == RuntimeBackendKind::LlamaCpp && requested_model.is_some() {
        if let Err(error) = select_cli_serve_model(&paths.models_dir, requested_model) {
            let kind = match &error {
                ModelSelectionError::UnknownModel { .. }
                | ModelSelectionError::MissingModelRequest { .. } => io::ErrorKind::InvalidInput,
                _ => io::ErrorKind::NotFound,
            };
            return Err(io::Error::new(
                kind,
                match error {
                    ModelSelectionError::UnknownModel { id } => {
                        format!("unknown model: {id}; check `loxa list`, then run `loxa pull {id}`")
                    }
                    ModelSelectionError::NotDownloaded { id } => {
                        format!("model not downloaded for {id}; run `loxa pull {id}`")
                    }
                    ModelSelectionError::NoDownloadedModels { suggested_id } => {
                        format!("no registry model is downloaded; run `loxa pull {suggested_id}`")
                    }
                    ModelSelectionError::MissingModelRequest { backend } => {
                        format!("--model <local-directory> is required with --engine {backend}")
                    }
                },
            ));
        }
    }
    Ok(())
}

fn select_cli_serve_model(
    models_dir: &Path,
    requested: Option<&str>,
) -> Result<&'static ModelEntry, ModelSelectionError> {
    if let Some(id) = requested {
        let entry = registry::find(id)
            .ok_or_else(|| ModelSelectionError::UnknownModel { id: id.to_string() })?;
        if !models_dir.join(entry.filename).is_file() {
            return Err(ModelSelectionError::NotDownloaded { id: id.to_string() });
        }
        return Ok(entry);
    }
    REGISTRY
        .iter()
        .find(|entry| models_dir.join(entry.filename).is_file())
        .ok_or_else(|| ModelSelectionError::NoDownloadedModels {
            suggested_id: REGISTRY[0].id.to_string(),
        })
}

fn exit_code_for_termination(outcome: RunTermination) -> ExitCode {
    match outcome {
        RunTermination::RequestedStop => ExitCode::SUCCESS,
        RunTermination::Interrupted => ExitCode::from(130),
        RunTermination::Failed | RunTermination::RecoveryRequired => ExitCode::from(1),
    }
}

struct CliLifecycleSink<'a, W, E> {
    stdout: &'a mut W,
    stderr: &'a mut E,
}

impl<W: Write, E: Write> LifecycleEventSink for CliLifecycleSink<'_, W, E> {
    fn emit(&mut self, event: LifecycleEvent) -> io::Result<()> {
        match event {
            LifecycleEvent::NodeListening { port, model_alias } => writeln!(
                self.stdout,
                "loxa node listening on http://127.0.0.1:{port} with model alias {model_alias}"
            ),
            LifecycleEvent::ModelReady { server } => print_run_ready(self.stdout, &server),
            LifecycleEvent::StableModelReady {
                model_id,
                port,
                model_alias,
            } => writeln!(
                self.stdout,
                "model {model_id} is ready on http://127.0.0.1:{port} with alias {model_alias}"
            ),
            LifecycleEvent::StableModelFailed {
                model_id,
                reason,
                recovery_required,
            } => {
                if recovery_required {
                    writeln!(
                        self.stderr,
                        "model {model_id} failed to start: {reason}; node recovery is required"
                    )
                } else {
                    writeln!(self.stderr, "model {model_id} failed to start: {reason}")
                }
            }
            LifecycleEvent::Restarting {
                process_label,
                before_healthy,
            } => {
                if before_healthy {
                    writeln!(
                        self.stdout,
                        "{process_label} exited before becoming healthy; restarting once..."
                    )
                } else {
                    writeln!(
                        self.stdout,
                        "{process_label} exited unexpectedly; restarting once..."
                    )
                }
            }
            LifecycleEvent::EngineExited {
                process_label,
                model_id,
                before_healthy,
                log_tail,
            } => {
                if before_healthy {
                    writeln!(
                        self.stderr,
                        "{process_label} exited before becoming healthy for {model_id}"
                    )?;
                } else {
                    writeln!(
                        self.stderr,
                        "{process_label} exited unexpectedly for {model_id}"
                    )?;
                }
                write_log_tail(self.stderr, &log_tail)
            }
            LifecycleEvent::HealthTimeout {
                process_label,
                log_path,
            } => {
                writeln!(
                    self.stderr,
                    "{process_label} did not become healthy within {} seconds",
                    supervisor::HEALTH_TIMEOUT.as_secs()
                )?;
                writeln!(self.stderr, "log file: {}", log_path.display())
            }
            LifecycleEvent::RecoveryRequired { run_id } => writeln!(
                self.stderr,
                "cleanup could not be confirmed for managed run {run_id}; recovery required"
            ),
        }
    }
}

fn print_run_ready<W: Write>(stdout: &mut W, server: &supervisor::ManagedServer) -> io::Result<()> {
    writeln!(stdout, "model id: {}", server.id)?;
    writeln!(stdout, "pid: {}", server.pid)?;
    writeln!(stdout, "port: {}", server.port)?;
    writeln!(stdout, "model path: {}", server.model_path.display())?;
    writeln!(
        stdout,
        "health url: http://127.0.0.1:{}/health",
        server.port
    )?;
    stdout.flush()
}

fn write_log_tail<W: Write>(writer: &mut W, log_tail: &str) -> io::Result<()> {
    if !log_tail.is_empty() {
        writeln!(writer, "log tail:\n{log_tail}")?;
    }
    Ok(())
}

fn supervisor_error_to_io(error: SupervisorError) -> io::Error {
    io::Error::other(error)
}

fn render_managed_servers<W: Write>(
    snapshot: Result<ManagedRunsSnapshot, SupervisorError>,
    stdout: &mut W,
) -> io::Result<ExitCode> {
    match snapshot.map_err(supervisor_error_to_io)? {
        ManagedRunsSnapshot::Missing => {
            writeln!(stdout, "no managed sidecars")?;
        }
        ManagedRunsSnapshot::Runs(rows) if rows.is_empty() => {
            writeln!(stdout, "no managed sidecars")?;
        }
        ManagedRunsSnapshot::Corrupt { message } => {
            writeln!(stdout, "managed sidecar state is corrupt: {message}")?;
        }
        ManagedRunsSnapshot::Legacy { path } => {
            writeln!(
                stdout,
                "legacy managed sidecar state requires manual recovery at {}; confirm no old Loxa process remains, then archive it manually",
                path.display()
            )?;
        }
        ManagedRunsSnapshot::Runs(rows) => {
            writeln!(
                stdout,
                "id                  pid    port   status               model"
            )?;
            for row in rows {
                let pid = row
                    .child_pid
                    .map(|pid| pid.to_string())
                    .unwrap_or_else(|| "-".into());
                let model_path = row
                    .model_path
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "-".into());
                writeln!(
                    stdout,
                    "{:<19} {:>6}  {:>5}  {:<19} {}",
                    row.model_id.as_deref().unwrap_or("-"),
                    pid,
                    row.port,
                    row.status,
                    model_path
                )?;
            }
        }
    }
    Ok(ExitCode::SUCCESS)
}

fn render_stop_outcome<W: Write, E: Write>(
    target: &str,
    outcome: Result<StopOutcome, SupervisorError>,
    stdout: &mut W,
    stderr: &mut E,
) -> io::Result<ExitCode> {
    match outcome.map_err(supervisor_error_to_io)? {
        StopOutcome::NoMatch if target == "all" => {
            writeln!(stdout, "no managed sidecars")?;
            Ok(ExitCode::SUCCESS)
        }
        StopOutcome::NoMatch => {
            writeln!(stderr, "no managed sidecar found for {target}")?;
            Ok(ExitCode::from(1))
        }
        StopOutcome::Completed { model_id } => {
            writeln!(
                stdout,
                "stop completed for {}",
                model_id.as_deref().unwrap_or("node")
            )?;
            Ok(ExitCode::SUCCESS)
        }
        StopOutcome::RecoveryRequired {
            run_id,
            model_id,
            owner_status,
        } => {
            writeln!(
                stderr,
                "stop requested for {}, but owner identity is {owner_status:?}; recovery required for {run_id}",
                model_id.as_deref().unwrap_or("node")
            )?;
            Ok(ExitCode::from(1))
        }
        StopOutcome::TimedOut { run_id, model_id } => {
            writeln!(
                stderr,
                "stop requested for {}, but the owner did not finish within {} seconds; recovery required for {run_id}",
                model_id.as_deref().unwrap_or("node"),
                supervisor::STOP_OWNER_WAIT_TIMEOUT.as_secs()
            )?;
            Ok(ExitCode::from(1))
        }
    }
}

fn validate_cli_contract(cli: &Cli) -> io::Result<()> {
    if matches!(
        &cli.command,
        Command::Run {
            ctx: Some(_),
            engine: RuntimeBackendKind::PyMlxLm,
            ..
        }
    ) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--ctx is not supported with --engine py-mlx-lm",
        ));
    }
    Ok(())
}

fn finish_cli_result<E: Write>(result: io::Result<ExitCode>, stderr: &mut E) -> ExitCode {
    match result {
        Ok(exit_code) => exit_code,
        Err(error) => {
            let _ = writeln!(stderr, "error: {error}");
            ExitCode::from(1)
        }
    }
}

fn run_calibration<W: Write>(stdout: &mut W) -> io::Result<ExitCode> {
    run_calibration_with(loxa_core::calibration::run_pinned_calibration, stdout)
}

fn run_calibration_with<W: Write>(
    execute: impl FnOnce() -> Result<
        loxa_core::calibration::CalibrationOutcome,
        loxa_core::calibration::CalibrationError,
    >,
    stdout: &mut W,
) -> io::Result<ExitCode> {
    match execute() {
        Ok(outcome) => {
            render_calibration_outcome(&outcome, stdout)?;
            Ok(ExitCode::SUCCESS)
        }
        Err(error) => Err(io::Error::other(calibration_error_message(&error))),
    }
}

fn render_calibration_outcome<W: Write>(
    outcome: &loxa_core::calibration::CalibrationOutcome,
    output: &mut W,
) -> io::Result<()> {
    use loxa_core::evidence::EvidenceVerdict;
    let evidence = &outcome.evidence;
    let evidence_path = outcome
        .evidence_path
        .as_deref()
        .filter(|path| path.is_absolute())
        .ok_or_else(|| {
            io::Error::other("calibration succeeded without an absolute evidence path")
        })?;
    writeln!(output, "workload: {}", evidence.workload_version)?;
    for (index, candidate) in evidence.candidates.iter().enumerate() {
        let label = if index == 0 { 'A' } else { 'B' };
        writeln!(
            output,
            "candidate {label}: {} fingerprint={} digest={} provider={:?}",
            candidate.identity.candidate_id,
            candidate.fingerprint,
            candidate.identity.artifact.digest_sha256,
            candidate.identity.provider_kind
        )?;
    }
    writeln!(output, "\nqualification:")?;
    for (index, candidate) in evidence.candidates.iter().enumerate() {
        let label = if index == 0 { 'A' } else { 'B' };
        let qualification = evidence
            .qualifications
            .iter()
            .find(|q| q.candidate_fingerprint == candidate.fingerprint);
        let passed = qualification.is_some_and(|q| q.passed_current_contract());
        let failure = qualification.and_then(|q| {
            q.case_results
                .iter()
                .find(|case| !case.passed)
                .and_then(|case| case.reason.as_deref())
                .or_else(|| q.failure_codes.first().map(String::as_str))
        });
        match failure {
            Some(reason) => writeln!(output, "  candidate {label}: failed — {reason}")?,
            None if passed => writeln!(output, "  candidate {label}: passed")?,
            None => writeln!(output, "  candidate {label}: failed — qualification_failed")?,
        }
    }
    match &evidence.verdict {
        EvidenceVerdict::Selected {
            candidate_id,
            reason_code,
            ..
        } => {
            let label = candidate_label(evidence, candidate_id)?;
            writeln!(output, "\nverdict: selected candidate {label}")?;
            writeln!(output, "reason: {reason_code}")?;
        }
        EvidenceVerdict::NoVerifiedPlan { reason_codes, .. } => {
            writeln!(output, "\nverdict: no verified plan")?;
            writeln!(output, "reason: {}", reason_codes.join(", "))?;
        }
        EvidenceVerdict::NoMaterialWinner {
            baseline_candidate_id,
            reason_code,
            ..
        } => {
            let label = candidate_label(evidence, baseline_candidate_id)?;
            writeln!(output, "\nverdict: no material winner")?;
            writeln!(output, "reason: {reason_code}")?;
            writeln!(output, "baseline retained: candidate {label}")?;
        }
    }
    writeln!(output, "evidence: {}", evidence_path.display())?;
    Ok(())
}

fn candidate_label(
    evidence: &loxa_core::evidence::CalibrationEvidence,
    id: &str,
) -> io::Result<char> {
    match evidence
        .candidates
        .iter()
        .position(|candidate| candidate.identity.candidate_id == id)
    {
        Some(0) => Ok('A'),
        Some(1) => Ok('B'),
        _ => Err(io::Error::other(format!(
            "verdict references unknown candidate id: {id:?}"
        ))),
    }
}

fn calibration_error_message(error: &loxa_core::calibration::CalibrationError) -> String {
    use loxa_core::calibration::CalibrationError;
    match error {
        CalibrationError::Isolation(reasons) => {
            format!("isolation prerequisite failed: {}", reasons.join(", "))
        }
        CalibrationError::Provider(error) => format!("provider prerequisite failed: {error}"),
        CalibrationError::IdentityChanged => {
            "evidence error: candidate identity changed during calibration".into()
        }
        CalibrationError::Evidence(error) => format!("evidence persistence failed: {error}"),
        CalibrationError::OperationAndTeardown {
            operation,
            teardown,
        } => format!(
            "{}; managed teardown also failed: {teardown}",
            calibration_error_message(operation)
        ),
        CalibrationError::Aborted {
            kind,
            evidence_path,
        } => format!(
            "calibration aborted: {kind}; evidence: {}",
            evidence_path.display()
        ),
    }
}

fn print_doctor<W: Write>(stdout: &mut W) -> io::Result<ExitCode> {
    write_doctor(stdout)?;
    Ok(ExitCode::SUCCESS)
}

fn write_doctor<W: Write>(stdout: &mut W) -> io::Result<()> {
    let hardware = HardwareReport::detect();
    let tools = LocalToolsReport::detect();
    write_doctor_report(stdout, &hardware, &tools)
}

fn write_doctor_report<W: Write>(
    stdout: &mut W,
    hardware: &HardwareReport,
    tools: &LocalToolsReport,
) -> io::Result<()> {
    writeln!(stdout, "Machine")?;
    writeln!(stdout, "  {:<16} {}", "Chip:", hardware.chip)?;
    writeln!(
        stdout,
        "  {:<16} {} physical / {} logical",
        "Cores:", hardware.physical_cores, hardware.logical_cores
    )?;
    writeln!(
        stdout,
        "  {:<16} {:.1} GB total / {:.1} GB available / {:.1} GB used",
        "RAM:",
        bytes_to_gb(hardware.ram_total_bytes),
        bytes_to_gb(hardware.ram_available_bytes),
        bytes_to_gb(hardware.ram_used_bytes)
    )?;
    writeln!(
        stdout,
        "  {:<16} {:.1} GB total / {:.1} GB used",
        "Swap:",
        bytes_to_gb(hardware.swap_total_bytes),
        bytes_to_gb(hardware.swap_used_bytes)
    )?;
    writeln!(
        stdout,
        "  {:<16} {} total / {} available",
        "Disk (/):",
        optional_bytes_to_gb(hardware.root_disk_total_bytes),
        optional_bytes_to_gb(hardware.root_disk_available_bytes)
    )?;
    writeln!(
        stdout,
        "  {:<16} {} {}",
        "OS:", hardware.os_name, hardware.os_version
    )?;
    writeln!(stdout)?;
    writeln!(stdout, "Detected tools")?;
    for tool in &tools.tools {
        write_tool(stdout, tool)?;
    }

    Ok(())
}

fn write_tool<W: Write>(stdout: &mut W, tool: &DetectedTool) -> io::Result<()> {
    let detection = &tool.detection;
    let evidence = if detection.evidence.is_empty() {
        "unknown".to_string()
    } else {
        detection.evidence.join("; ")
    };

    writeln!(
        stdout,
        "  {:<10} {:<13} {:<11} {}",
        tool.name, detection.install_state, detection.run_state, evidence
    )
}

fn bytes_to_gb(bytes: u64) -> f64 {
    bytes as f64 / 1024.0 / 1024.0 / 1024.0
}

fn optional_bytes_to_gb(bytes: Option<u64>) -> String {
    bytes
        .map(|bytes| format!("{:.1} GB", bytes_to_gb(bytes)))
        .unwrap_or_else(|| "unknown".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use loxa_core::detect::{InstallState, RunState, ToolDetection};
    use loxa_core::registry::REGISTRY;
    use std::fs;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn stable_startup_events_render_truthful_ready_and_sanitized_failures() {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        {
            let mut sink = CliLifecycleSink {
                stdout: &mut stdout,
                stderr: &mut stderr,
            };
            sink.emit(LifecycleEvent::StableModelReady {
                model_id: "gemma-3-4b-it-q4".into(),
                port: 11_435,
                model_alias: "loxa".into(),
            })
            .unwrap();
            for (reason, recovery_required) in [
                ("model artifact is not downloaded and verified", false),
                ("engine readiness failed safely", false),
                ("model startup was cancelled", false),
                ("node recovery is required", true),
            ] {
                sink.emit(LifecycleEvent::StableModelFailed {
                    model_id: "gemma-3-4b-it-q4".into(),
                    reason: reason.into(),
                    recovery_required,
                })
                .unwrap();
            }
        }

        let stdout = String::from_utf8(stdout).unwrap();
        let stderr = String::from_utf8(stderr).unwrap();
        assert!(stdout.contains("model gemma-3-4b-it-q4 is ready"));
        assert!(stdout.contains("alias loxa"));
        assert!(stderr.contains("not downloaded and verified"));
        assert!(stderr.contains("readiness failed safely"));
        assert!(stderr.contains("startup was cancelled"));
        assert!(stderr.contains("node recovery is required"));
        assert!(!stderr.contains("/Users/"));
        assert!(!stderr.contains("127.0.0.1:"));
    }

    #[test]
    fn doctor_report_renders_injected_python_mlx_evidence() {
        let hardware = HardwareReport {
            chip: "Apple M4".to_string(),
            physical_cores: 4,
            logical_cores: 8,
            ram_total_bytes: 16 * 1024 * 1024 * 1024,
            ram_available_bytes: 8 * 1024 * 1024 * 1024,
            ram_used_bytes: 8 * 1024 * 1024 * 1024,
            swap_total_bytes: 0,
            swap_used_bytes: 0,
            root_disk_total_bytes: Some(512 * 1024 * 1024 * 1024),
            root_disk_available_bytes: Some(256 * 1024 * 1024 * 1024),
            os_name: "macOS".to_string(),
            os_version: "15.0".to_string(),
        };
        let tools = LocalToolsReport {
            tools: vec![DetectedTool {
                name: "Python MLX (external)".to_string(),
                detection: ToolDetection {
                    install_state: InstallState::Installed,
                    run_state: RunState::ReachableUnverified,
                    evidence: vec![
                        "platform compatible: macos/aarch64".to_string(),
                        "server path: /opt/tools/mlx_lm.server".to_string(),
                        "required version: 0.31.3".to_string(),
                        "detected version: 0.31.3".to_string(),
                        "external default endpoint reachable: 127.0.0.1:8080".to_string(),
                    ],
                },
            }],
        };
        let mut output = Vec::new();

        write_doctor_report(&mut output, &hardware, &tools).expect("render doctor report");

        let output = String::from_utf8(output).expect("doctor output is utf8");
        assert!(output.contains("Python MLX"));
        assert!(output.contains("platform compatible: macos/aarch64"));
        assert!(output.contains("server path: /opt/tools/mlx_lm.server"));
        assert!(output.contains("required version: 0.31.3"));
        assert!(output.contains("detected version: 0.31.3"));
        assert!(output.contains("reachable (unverified)"));
        assert!(output.contains("external default endpoint reachable: 127.0.0.1:8080"));
    }

    fn persist_run_for_server(state_path: &Path, server: &ManagedServer) -> supervisor::ManagedRun {
        let run_id = format!("test-run-{}", server.pid);
        let mut run = starting_run_for_test(state_path, &run_id);
        run.model_id = Some(server.id.clone());
        run.port = server.port;
        run.generation_alias = format!("loxa-{run_id}-g0");
        run.log_path = state_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(format!("{run_id}.log"));
        supervisor::create_starting_run(state_path, run.clone()).expect("create starting run");
        let starting_identity = run.identity();
        run.lifecycle = supervisor::RunLifecycle::Running;
        run.child_pid = Some(server.pid);
        run.child_process_start_time_unix_s = server.process_start_time_unix_s;
        assert!(
            supervisor::update_runtime_state_run(state_path, &starting_identity, run.clone())
                .expect("attach test child")
        );
        run
    }

    fn starting_run_for_test(state_path: &Path, run_id: &str) -> supervisor::ManagedRun {
        supervisor::ManagedRun {
            schema_version: supervisor::RUNTIME_STATE_SCHEMA_VERSION,
            run_id: run_id.to_string(),
            model_id: Some("gemma-3-4b-it-q4".to_string()),
            owner_pid: 42,
            owner_process_start_time_unix_s: 456,
            stop_requested: false,
            lifecycle: supervisor::RunLifecycle::Starting,
            generation: 0,
            generation_alias: format!("loxa-{run_id}-g0"),
            control_port: None,
            port: 8080,
            log_path: state_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join(format!("{run_id}.log")),
            child_pid: None,
            child_process_start_time_unix_s: None,
            child_pgid: None,
        }
    }

    fn set_test_owner_to_current_process(run: &mut supervisor::ManagedRun) {
        run.owner_pid = std::process::id();
        run.owner_process_start_time_unix_s =
            supervisor::process_start_time_with_retry(run.owner_pid)
                .expect("current test process start time");
    }

    fn set_test_child_to_current_process(run: &mut supervisor::ManagedRun, listener: &TcpListener) {
        run.lifecycle = supervisor::RunLifecycle::Running;
        run.port = listener.local_addr().expect("listener address").port();
        run.child_pid = Some(std::process::id());
        run.child_process_start_time_unix_s = Some(
            supervisor::process_start_time_with_retry(std::process::id())
                .expect("current test child process start time"),
        );
    }

    #[test]
    fn live_control_keeps_targeting_the_stable_node_endpoint_after_engine_load() {
        let temp = TempDir::new("live-control-stable-endpoint");
        let state_path = temp.path().join("run").join("managed.json");
        let mut run = starting_run_for_test(&state_path, "stable-owner");
        run.control_port = Some(45_678);
        run.port = 45_679;
        run.lifecycle = supervisor::RunLifecycle::Running;

        assert_eq!(
            live_control_address(&run).unwrap(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 45_678)
        );
        assert_ne!(run.control_port, Some(run.port));
    }

    #[test]
    fn live_control_fails_closed_when_migrated_state_has_no_proven_control_endpoint() {
        let temp = TempDir::new("live-control-missing-endpoint");
        let run = starting_run_for_test(&temp.path().join("managed.json"), "old-owner");

        let error = live_control_address(&run).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        assert!(error.to_string().contains("start `loxa serve`"));
    }

    #[test]
    fn live_control_reports_missing_migrated_endpoint_before_loading_credentials() {
        let temp = TempDir::new("live-control-migrated-missing-endpoint");
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("run").join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };
        let engine_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let mut run = starting_run_for_test(&paths.state_path, "migrated-live-owner");
        run.log_path = PathBuf::from("managed.log");
        set_test_owner_to_current_process(&mut run);
        set_test_child_to_current_process(&mut run, &engine_listener);
        fs::create_dir_all(paths.state_path.parent().unwrap()).unwrap();
        fs::write(
            &paths.state_path,
            format!(
                r#"{{
  "schema_version": 3,
  "runs": [{{
    "schema_version": 3,
    "run_id": "{}",
    "model_id": "{}",
    "owner_pid": {},
    "owner_process_start_time_unix_s": {},
    "stop_requested": false,
    "lifecycle": "running",
    "generation": {},
    "generation_alias": "{}",
    "port": {},
    "log_path": "{}",
    "child_pid": {},
    "child_process_start_time_unix_s": {},
    "child_pgid": null
  }}]
}}"#,
                run.run_id,
                run.model_id.as_deref().unwrap(),
                run.owner_pid,
                run.owner_process_start_time_unix_s,
                run.generation,
                run.generation_alias,
                run.port,
                run.log_path.display(),
                run.child_pid.unwrap(),
                run.child_process_start_time_unix_s.unwrap(),
            ),
        )
        .unwrap();

        let token_path = temp.path().join("control.token");
        assert!(!token_path.exists());
        let error = live_control(&paths).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        assert!(error.to_string().contains("start `loxa serve`"));
        assert!(!token_path.exists());
    }

    #[test]
    fn live_control_learns_opaque_instance_and_keeps_first_bearer_on_the_proved_socket() {
        use loxa_core::control::contracts::NodeStatus;
        #[cfg(unix)]
        use std::os::unix::fs::PermissionsExt;

        let temp = TempDir::new("live-control-custom-endpoint");
        #[cfg(unix)]
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("run").join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };
        let token = ControlToken::load_or_create(&temp.path().join("control.token")).unwrap();
        let control_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let control_port = control_listener.local_addr().unwrap().port();
        let engine_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let mut run = starting_run_for_test(&paths.state_path, "stable-custom-owner");
        set_test_owner_to_current_process(&mut run);
        run.control_port = Some(control_port);
        set_test_child_to_current_process(&mut run, &engine_listener);
        let runtime_identity = "550e8400-e29b-41d4-a716-446655440001".to_string();
        assert_ne!(runtime_identity, run.run_id);
        persist_test_run(&paths.state_path, run.clone());

        let server_token = token.clone();
        let proof_server = std::thread::spawn(move || {
            let (mut socket, _) = control_listener.accept().unwrap();
            let mut request = Vec::new();
            let mut byte = [0_u8; 1];
            while !request.ends_with(b"\r\n\r\n") {
                socket.read_exact(&mut byte).unwrap();
                request.push(byte[0]);
            }
            let request = String::from_utf8(request).unwrap();
            assert!(!request.to_ascii_lowercase().contains("authorization:"));
            let nonce = request
                .lines()
                .find_map(|line| line.strip_prefix("X-Loxa-Challenge: "))
                .unwrap();
            let proof = server_token
                .node_identity_proof(nonce, "test-node", &runtime_identity, NodeStatus::Ready)
                .unwrap();
            let body = format!(
                r#"{{"protocol_version":1,"node_id":"test-node","runtime_identity":"{runtime_identity}","status":"ready","challenge_proof":"{proof}"}}"#
            );
            write!(
                socket,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();

            let mut authenticated_request = Vec::new();
            while !authenticated_request.ends_with(b"\r\n\r\n") {
                socket.read_exact(&mut byte).unwrap();
                authenticated_request.push(byte[0]);
            }
            let request = String::from_utf8(authenticated_request).unwrap();
            assert!(request.starts_with("GET /loxa/v1/models "));
            assert!(request
                .to_ascii_lowercase()
                .contains("authorization: bearer "));
            write!(
                socket,
                "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n[]"
            )
            .unwrap();
            control_listener.set_nonblocking(true).unwrap();
            assert!(
                control_listener.accept().is_err(),
                "first bearer opened a replacement socket"
            );
        });

        let client = live_control(&paths).unwrap().unwrap();
        assert!(client.models().unwrap().is_empty());
        proof_server.join().unwrap();
        let RuntimeStateRead::Loaded(stored) =
            supervisor::read_runtime_state(&paths.state_path).unwrap()
        else {
            panic!("persisted loaded state")
        };
        assert_eq!(stored[0].control_port, Some(control_port));
        assert_eq!(stored[0].port, engine_listener.local_addr().unwrap().port());
        assert_ne!(stored[0].control_port, Some(stored[0].port));
    }

    #[test]
    fn live_control_drops_proved_socket_when_full_managed_state_changes_before_recheck() {
        use loxa_core::control::contracts::NodeStatus;
        use std::sync::mpsc;

        let temp = TempDir::new("live-control-state-race");
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("run").join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };
        let token = ControlToken::load_or_create(&temp.path().join("control.token")).unwrap();
        let control_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let mut run = starting_run_for_test(&paths.state_path, "state-race-owner");
        set_test_owner_to_current_process(&mut run);
        run.control_port = Some(control_listener.local_addr().unwrap().port());
        run.lifecycle = supervisor::RunLifecycle::Unloaded;
        run.child_pid = None;
        run.child_process_start_time_unix_s = None;
        persist_test_run(&paths.state_path, run.clone());

        let state_path = paths.state_path.clone();
        let server_token = token.clone();
        let (request_tx, request_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            let (mut socket, _) = control_listener.accept().unwrap();
            let mut request = Vec::new();
            let mut byte = [0_u8; 1];
            while !request.ends_with(b"\r\n\r\n") {
                socket.read_exact(&mut byte).unwrap();
                request.push(byte[0]);
            }
            let request = String::from_utf8(request).unwrap();
            let nonce = request
                .lines()
                .find_map(|line| line.strip_prefix("X-Loxa-Challenge: "))
                .unwrap();
            let proof = server_token
                .node_identity_proof(
                    nonce,
                    "opaque-node",
                    "opaque-instance",
                    NodeStatus::Unloaded,
                )
                .unwrap();
            let body = format!(
                r#"{{"protocol_version":1,"node_id":"opaque-node","runtime_identity":"opaque-instance","status":"unloaded","challenge_proof":"{proof}"}}"#
            );
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{body}",
                body.len()
            );
            socket
                .write_all(&response.as_bytes()[..response.len() - 1])
                .unwrap();

            let mut replacement = run;
            replacement.generation_alias = "changed-after-proof".into();
            assert!(supervisor::update_runtime_state_run(
                &state_path,
                &replacement.identity(),
                replacement,
            )
            .unwrap());
            socket
                .write_all(&response.as_bytes()[response.len() - 1..])
                .unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            request_tx.send(socket.read(&mut byte).unwrap()).unwrap();
        });

        let error = live_control(&paths).unwrap_err();
        assert!(error
            .to_string()
            .contains("state changed during peer proof"));
        assert_eq!(request_rx.recv_timeout(Duration::from_secs(2)).unwrap(), 0);
        worker.join().unwrap();
    }

    fn persist_test_run(state_path: &Path, run: supervisor::ManagedRun) {
        let mut starting = run.clone();
        starting.stop_requested = false;
        starting.lifecycle = supervisor::RunLifecycle::Starting;
        starting.child_pid = None;
        starting.child_process_start_time_unix_s = None;
        starting.child_pgid = None;
        supervisor::create_starting_run(state_path, starting.clone())
            .expect("create test starting run");
        if run != starting {
            assert!(
                supervisor::update_runtime_state_run(state_path, &starting.identity(), run)
                    .expect("persist final test run")
            );
        }
    }

    fn render_ps_for_test(temp: &TempDir) -> String {
        let state_path = temp.path().join("managed.json");
        let before = fs::read(&state_path).expect("read state before ps");
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: state_path.clone(),
            logs_dir: temp.path().join("logs"),
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        let exit = run_with_paths(
            Cli {
                command: Command::Ps,
            },
            &paths,
            &mut stdout,
            &mut stderr,
        );

        assert_eq!(exit, std::process::ExitCode::SUCCESS);
        assert!(stderr.is_empty());
        assert_eq!(
            fs::read(&state_path).expect("read state after ps"),
            before,
            "ps must not mutate managed state"
        );
        String::from_utf8(stdout).expect("stdout is utf8")
    }

    fn only_ps_row_fields(stdout: &str) -> Vec<&str> {
        let lines = stdout.lines().collect::<Vec<_>>();
        assert_eq!(
            lines.len(),
            2,
            "expected one header and one row: {stdout:?}"
        );
        lines[1].split_whitespace().collect()
    }

    #[test]
    fn serve_selects_first_downloaded_registry_model_in_order() {
        let temp = TempDir::new("serve-selection");
        let later = &REGISTRY[2];
        let first = &REGISTRY[1];
        fs::write(temp.path().join(later.filename), b"later").unwrap();
        fs::write(temp.path().join(first.filename), b"first").unwrap();

        let selected = select_cli_serve_model(temp.path(), None).unwrap();

        assert_eq!(selected.id, first.id);
    }

    #[test]
    fn serve_selection_error_remains_product_neutral() {
        let temp = TempDir::new("serve-selection");

        let error = match select_cli_serve_model(temp.path(), Some("not-in-registry")) {
            Ok(_) => panic!("unknown model unexpectedly selected"),
            Err(error) => error,
        };

        assert_eq!(
            error,
            ModelSelectionError::UnknownModel {
                id: "not-in-registry".into()
            }
        );
        assert!(!error.to_string().contains("loxa pull"));
    }

    #[test]
    fn clap_parses_serve_options() {
        let cli = Cli::try_parse_from([
            "loxa",
            "serve",
            "--model",
            "gemma-3-4b-it-q4",
            "--port",
            "11435",
        ])
        .unwrap();
        match cli.command {
            Command::Serve {
                model,
                port,
                engine,
            } => {
                assert_eq!(model.as_deref(), Some("gemma-3-4b-it-q4"));
                assert_eq!(port, Some(11435));
                assert_eq!(engine, RuntimeBackendKind::LlamaCpp);
            }
            _ => panic!("expected serve command"),
        }
    }

    #[test]
    fn clap_preserves_llama_default_and_accepts_explicit_runtime_engines() {
        assert!(matches!(
            Cli::try_parse_from(["loxa", "run", "gemma-3-4b-it-q4"]),
            Ok(Cli {
                command: Command::Run {
                    engine: RuntimeBackendKind::LlamaCpp,
                    ..
                }
            })
        ));
        assert!(matches!(
            Cli::try_parse_from([
                "loxa",
                "run",
                "/tmp/mlx model",
                "--engine",
                "py-mlx-lm",
            ]),
            Ok(Cli {
                command: Command::Run {
                    id,
                    engine: RuntimeBackendKind::PyMlxLm,
                    ..
                }
            }) if id == "/tmp/mlx model"
        ));
        assert!(matches!(
            Cli::try_parse_from([
                "loxa",
                "serve",
                "--model",
                "/tmp/mlx model",
                "--engine",
                "py-mlx-lm",
            ]),
            Ok(Cli {
                command: Command::Serve {
                    model: Some(model),
                    engine: RuntimeBackendKind::PyMlxLm,
                    ..
                }
            }) if model == "/tmp/mlx model"
        ));
        assert!(Cli::try_parse_from(["loxa", "serve", "--engine", "llama-cpp",]).is_ok());
    }

    #[test]
    fn clap_rejects_invalid_engine() {
        assert!(Cli::try_parse_from([
            "loxa",
            "run",
            "gemma-3-4b-it-q4",
            "--engine",
            "not-an-engine",
        ])
        .is_err());
    }

    #[test]
    fn python_ctx_is_rejected_before_execution() {
        let cli = Cli::try_parse_from([
            "loxa",
            "run",
            "/tmp/mlx-model",
            "--engine",
            "py-mlx-lm",
            "--ctx",
            "4096",
        ])
        .expect("parse Python engine request");
        let temp = TempDir::new("loxa-python-ctx");
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        let exit = run_with_paths(cli, &paths, &mut stdout, &mut stderr);

        assert_eq!(exit, ExitCode::from(1));
        assert!(stdout.is_empty());
        let error = String::from_utf8(stderr).expect("stderr is utf8");
        assert!(error.contains("--ctx"));
        assert!(error.contains("py-mlx-lm"));
        assert!(!paths.state_path.exists());
    }

    #[test]
    fn clap_parses_all_subcommands() {
        assert!(matches!(
            Cli::try_parse_from(["loxa", "calibrate"]),
            Ok(Cli {
                command: Command::Calibrate
            })
        ));
        assert!(Cli::try_parse_from(["loxa", "doctor"]).is_ok());
        assert!(Cli::try_parse_from(["loxa", "list"]).is_ok());
        assert!(Cli::try_parse_from(["loxa", "pull", "gemma-3-4b-it-q4"]).is_ok());
        assert!(Cli::try_parse_from(["loxa", "rm", "gemma-3-4b-it-q4"]).is_ok());
        assert!(matches!(
            Cli::try_parse_from(["loxa", "load", "gemma-3-4b-it-q4"]),
            Ok(Cli { command: Command::Load { id } }) if id == "gemma-3-4b-it-q4"
        ));
        assert!(matches!(
            Cli::try_parse_from(["loxa", "unload"]),
            Ok(Cli {
                command: Command::Unload
            })
        ));
        assert!(matches!(
            Cli::try_parse_from(["loxa", "run", "gemma-3-4b-it-q4", "--ctx", "4096", "--port", "9000"]),
            Ok(Cli {
                command: Command::Run {
                    id,
                    ctx: Some(4096),
                    port: Some(9000),
                    engine: RuntimeBackendKind::LlamaCpp,
                },
            }) if id == "gemma-3-4b-it-q4"
        ));
        assert!(matches!(
            Cli::try_parse_from(["loxa", "ps"]),
            Ok(Cli {
                command: Command::Ps,
            })
        ));
        assert!(matches!(
            Cli::try_parse_from(["loxa", "stop", "all"]),
            Ok(Cli {
                command: Command::Stop { target },
            }) if target == "all"
        ));
        assert!(matches!(
            Cli::try_parse_from(["loxa", "chat", "hello"]),
            Ok(Cli {
                command: Command::Chat { chat: None, prompt }
            }) if prompt == "hello"
        ));
        assert!(matches!(
            Cli::try_parse_from(["loxa", "chat", "--chat", "0123456789abcdef0123456789abcdef", "follow up"]),
            Ok(Cli {
                command: Command::Chat { chat: Some(chat), prompt }
            }) if chat == "0123456789abcdef0123456789abcdef" && prompt == "follow up"
        ));
        assert!(Cli::try_parse_from([
            "loxa", "chats", "list", "--limit", "50", "--before", "cursor_1", "--json"
        ])
        .is_ok());
        assert!(Cli::try_parse_from([
            "loxa",
            "chats",
            "show",
            "0123456789abcdef0123456789abcdef",
            "--json"
        ])
        .is_ok());
        assert!(Cli::try_parse_from([
            "loxa",
            "chats",
            "rename",
            "0123456789abcdef0123456789abcdef",
            "A title"
        ])
        .is_ok());
        assert!(Cli::try_parse_from([
            "loxa",
            "chats",
            "delete",
            "0123456789abcdef0123456789abcdef",
            "--yes"
        ])
        .is_ok());
        assert!(Cli::try_parse_from(["loxa", "chats", "clear", "--yes"]).is_ok());
    }

    #[test]
    fn destructive_history_commands_require_explicit_confirmation() {
        assert_eq!(
            require_confirmation(false, "delete"),
            Err("refusing to delete chat history without --yes")
        );
        assert_eq!(require_confirmation(true, "delete"), Ok(()));
    }

    #[test]
    fn segmented_messages_are_joined_in_order_for_cli_show() {
        let pages = vec![
            loxa_core::control::client::MessagePageView {
                message_id: "11111111111111111111111111111111".into(),
                turn_id: "22222222222222222222222222222222".into(),
                role: "assistant".into(),
                segment_count: 2,
                segments: vec![loxa_core::control::client::MessageSegmentView {
                    message_id: "11111111111111111111111111111111".into(),
                    turn_id: "22222222222222222222222222222222".into(),
                    role: "assistant".into(),
                    segment_index: 0,
                    segment_count: 2,
                    content: "first ".into(),
                }],
                next_segment: Some(1),
            },
            loxa_core::control::client::MessagePageView {
                message_id: "11111111111111111111111111111111".into(),
                turn_id: "22222222222222222222222222222222".into(),
                role: "assistant".into(),
                segment_count: 2,
                segments: vec![loxa_core::control::client::MessageSegmentView {
                    message_id: "11111111111111111111111111111111".into(),
                    turn_id: "22222222222222222222222222222222".into(),
                    role: "assistant".into(),
                    segment_index: 1,
                    segment_count: 2,
                    content: "second".into(),
                }],
                next_segment: None,
            },
        ];

        let summary = loxa_core::control::client::MessageSummaryView {
            id: "11111111111111111111111111111111".into(),
            turn_id: "22222222222222222222222222222222".into(),
            role: "assistant".into(),
            content_bytes: 12,
            created_at_ms: 1,
            updated_at_ms: 2,
        };
        assert_eq!(
            join_message_pages(&summary, &pages).unwrap(),
            "first second"
        );

        let mut invalid = pages;
        invalid[1].segments[0].role = "user".into();
        assert!(join_message_pages(&summary, &invalid).is_err());
    }

    #[test]
    fn multi_page_turns_require_matching_chat_unique_ids_and_ascending_ordinals() {
        let chat = "0123456789abcdef0123456789abcdef";
        let turn = |id: &str, ordinal| loxa_core::control::client::TurnView {
            id: id.into(),
            chat_id: chat.into(),
            ordinal,
            state: "completed".into(),
            provenance: loxa_core::control::client::TurnProvenanceView {
                model_alias: "loxa".into(),
                recipe_id: "recipe".into(),
                engine_name: None,
                engine_version: None,
            },
            error_code: None,
            metrics: loxa_core::control::client::TurnMetricsView::default(),
            created_at_ms: ordinal,
            updated_at_ms: ordinal,
        };
        let mut turns = Vec::new();
        append_turn_page(
            chat,
            &mut turns,
            vec![turn("11111111111111111111111111111111", 1)],
        )
        .unwrap();
        append_turn_page(
            chat,
            &mut turns,
            vec![turn("22222222222222222222222222222222", 2)],
        )
        .unwrap();
        assert_eq!(turns.len(), 2);
        assert!(append_turn_page(
            chat,
            &mut turns,
            vec![turn("11111111111111111111111111111111", 3)]
        )
        .is_err());
        assert!(append_turn_page(
            chat,
            &mut turns,
            vec![turn("33333333333333333333333333333333", 2)]
        )
        .is_err());
        let mut wrong_chat = turn("44444444444444444444444444444444", 3);
        wrong_chat.chat_id = "ffffffffffffffffffffffffffffffff".into();
        assert!(append_turn_page(chat, &mut turns, vec![wrong_chat]).is_err());
    }

    #[test]
    fn load_without_a_live_node_fails_without_creating_runtime_or_model_state() {
        let temp = TempDir::new("offline-load");
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("run").join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };
        let cli = Cli::try_parse_from(["loxa", "load", "gemma-3-4b-it-q4"]).unwrap();
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        assert_eq!(
            run_with_paths(cli, &paths, &mut stdout, &mut stderr),
            ExitCode::from(1)
        );
        assert!(stdout.is_empty());
        assert!(String::from_utf8(stderr)
            .unwrap()
            .contains("no managed node"));
        assert!(!paths.state_path.exists());
        assert!(!paths.models_dir.exists());
    }

    #[test]
    fn chat_history_without_a_live_node_is_actionable_and_never_opens_storage() {
        let temp = TempDir::new("offline-history");
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("run").join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };
        let cli = Cli::try_parse_from(["loxa", "chats", "list"]).unwrap();
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        assert_eq!(
            run_with_paths(cli, &paths, &mut stdout, &mut stderr),
            ExitCode::from(1)
        );
        assert!(stdout.is_empty());
        let stderr = String::from_utf8(stderr).unwrap();
        assert!(stderr.contains("start one with `loxa serve`"));
        assert!(!paths.state_path.exists());
        assert!(!temp.path().join("chat-history.sqlite3").exists());
    }

    #[test]
    fn corrupt_live_owner_state_fails_closed_before_rm_can_mutate_models() {
        let temp = TempDir::new("corrupt-live-rm");
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("run").join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };
        fs::create_dir_all(paths.state_path.parent().unwrap()).unwrap();
        fs::create_dir_all(&paths.models_dir).unwrap();
        let artifact = paths.models_dir.join(REGISTRY[0].filename);
        fs::write(&artifact, b"sentinel").unwrap();
        fs::write(&paths.state_path, b"not-json").unwrap();
        let cli = Cli::try_parse_from(["loxa", "rm", REGISTRY[0].id]).unwrap();
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        assert_eq!(
            run_with_paths(cli, &paths, &mut stdout, &mut stderr),
            ExitCode::from(1)
        );
        assert_eq!(fs::read(&artifact).unwrap(), b"sentinel");
        assert!(String::from_utf8(stderr).unwrap().contains("corrupt"));
    }

    #[test]
    fn dead_managed_owner_fails_closed_before_rm_can_mutate_models() {
        let temp = TempDir::new("dead-live-rm");
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("run").join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };
        fs::create_dir_all(&paths.models_dir).unwrap();
        let artifact = paths.models_dir.join(REGISTRY[0].filename);
        fs::write(&artifact, b"sentinel").unwrap();
        let mut run = starting_run_for_test(&paths.state_path, "dead-owner");
        run.lifecycle = supervisor::RunLifecycle::Unloaded;
        supervisor::create_starting_run(&paths.state_path, run).unwrap();
        let cli = Cli::try_parse_from(["loxa", "rm", REGISTRY[0].id]).unwrap();
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        assert_eq!(
            run_with_paths(cli, &paths, &mut stdout, &mut stderr),
            ExitCode::from(1)
        );
        assert_eq!(fs::read(&artifact).unwrap(), b"sentinel");
        assert!(String::from_utf8(stderr)
            .unwrap()
            .contains("recovery required"));
    }

    fn assert_offline_model_mutation_excludes_owner_start(use_pull: bool) {
        let temp = TempDir::new(if use_pull {
            "pull-owner-race"
        } else {
            "rm-owner-race"
        });
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("run").join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };
        let mutation_paths = paths.clone();
        let artifact = temp.path().join("mutation-finished");
        let mutation_artifact = artifact.clone();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let mutation = std::thread::spawn(move || {
            let execute = || {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                fs::write(&mutation_artifact, b"complete")?;
                Ok(ExitCode::SUCCESS)
            };
            if use_pull {
                offline_pull_with(&mutation_paths, execute)
            } else {
                offline_rm_with(&mutation_paths, execute)
            }
        });
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();

        let owner_state_path = paths.state_path.clone();
        let (owner_tx, owner_rx) = std::sync::mpsc::channel();
        let owner = std::thread::spawn(move || {
            let run = starting_run_for_test(&owner_state_path, "racing-owner");
            owner_tx
                .send(supervisor::create_starting_run(&owner_state_path, run))
                .unwrap();
        });
        assert!(owner_rx.recv_timeout(Duration::from_millis(100)).is_err());
        assert!(!paths.state_path.exists());

        release_tx.send(()).unwrap();
        assert_eq!(mutation.join().unwrap().unwrap(), ExitCode::SUCCESS);
        assert_eq!(fs::read(&artifact).unwrap(), b"complete");
        assert!(owner_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .is_ok());
        owner.join().unwrap();
    }

    #[test]
    fn offline_pull_excludes_a_concurrent_managed_owner_start() {
        assert_offline_model_mutation_excludes_owner_start(true);
    }

    #[test]
    fn offline_rm_excludes_a_concurrent_managed_owner_start() {
        assert_offline_model_mutation_excludes_owner_start(false);
    }

    #[test]
    fn calibration_renderer_reports_no_material_winner_and_retained_baseline() {
        let outcome = calibration_outcome_for_test(
            loxa_core::evidence::EvidenceVerdict::NoMaterialWinner {
                schema_version: 1,
                baseline_candidate_id: "gemma-3-4b-it-q4".into(),
                reason_code: "no_material_winner".into(),
            },
            loxa_core::selector::SelectorVerdict::NoMaterialWinner {
                schema_version: 1,
                baseline_candidate_id: "gemma-3-4b-it-q4".into(),
                reason: "both qualified; attached candidate did not clear both thresholds".into(),
            },
        );
        let evidence_path = outcome
            .evidence_path
            .as_ref()
            .unwrap()
            .display()
            .to_string();
        let mut output = Vec::new();
        render_calibration_outcome(&outcome, &mut output).unwrap();
        let output = String::from_utf8(output).unwrap();
        assert!(output.contains("workload: tool-use-v1"));
        assert!(output.contains("verdict: no material winner"));
        assert!(output.contains("baseline retained: candidate A"));
        assert!(output.contains(&format!("evidence: {evidence_path}")));
        assert!(!output.contains("best"));
    }

    #[test]
    fn calibration_renderer_reports_selected_and_no_verified_outcomes() {
        use loxa_core::evidence::EvidenceVerdict;
        use loxa_core::selector::SelectorVerdict;
        let cases = [
            (
                EvidenceVerdict::Selected {
                    schema_version: 1,
                    candidate_id: "gemma-3-4b-it-q4".into(),
                    reason_code: "only_managed_qualified".into(),
                },
                SelectorVerdict::Selected {
                    schema_version: 1,
                    candidate_id: "gemma-3-4b-it-q4".into(),
                    reason: "only managed qualified".into(),
                },
                "verdict: selected candidate A",
            ),
            (
                EvidenceVerdict::Selected {
                    schema_version: 1,
                    candidate_id: "candidate-b".into(),
                    reason_code: "only_attached_qualified".into(),
                },
                SelectorVerdict::Selected {
                    schema_version: 1,
                    candidate_id: "candidate-b".into(),
                    reason: "only attached qualified".into(),
                },
                "verdict: selected candidate B",
            ),
            (
                EvidenceVerdict::NoVerifiedPlan {
                    schema_version: 1,
                    reason_codes: vec!["qualification_failed".into()],
                },
                SelectorVerdict::NoVerifiedPlan {
                    schema_version: 1,
                    reasons: vec!["qualification failed".into()],
                },
                "verdict: no verified plan",
            ),
        ];
        for (evidence_verdict, verdict, expected) in cases {
            let outcome = calibration_outcome_for_test(evidence_verdict, verdict);
            let evidence_path = outcome
                .evidence_path
                .as_ref()
                .unwrap()
                .display()
                .to_string();
            let mut output = Vec::new();
            render_calibration_outcome(&outcome, &mut output).unwrap();
            let output = String::from_utf8(output).unwrap();
            assert!(output.contains(expected));
            assert!(output.contains("candidate A: gemma-3-4b-it-q4 fingerprint="));
            assert!(output.contains("digest="));
            assert!(output.contains("provider=Ollama"));
            assert!(output.contains("candidate A: passed"));
            assert!(output.contains("candidate B: passed"));
            assert!(output.contains("reason:"));
            assert!(output.contains(&format!("evidence: {evidence_path}")));
        }
    }

    #[test]
    fn calibration_errors_are_nonzero_and_name_the_prerequisite_class() {
        use loxa_core::calibration::CalibrationError;
        use loxa_core::evidence::read_evidence_json;
        use loxa_core::provider::ProviderError;
        let evidence_error = read_evidence_json(b"not-json").unwrap_err();
        let cases = [
            (
                CalibrationError::Isolation(vec!["other model loaded".into()]),
                "isolation prerequisite failed",
            ),
            (
                CalibrationError::Provider(ProviderError::Unreachable),
                "provider prerequisite failed",
            ),
            (CalibrationError::IdentityChanged, "evidence error"),
            (
                CalibrationError::Evidence(evidence_error),
                "evidence persistence failed",
            ),
            (
                CalibrationError::OperationAndTeardown {
                    operation: Box::new(CalibrationError::Provider(ProviderError::Unreachable)),
                    teardown: ProviderError::Lifecycle("cleanup".into()),
                },
                "managed teardown also failed",
            ),
            (
                CalibrationError::Aborted {
                    kind: "isolation_lost".into(),
                    evidence_path: PathBuf::from("/tmp/aborted-evidence.json"),
                },
                "calibration aborted: isolation_lost; evidence: /tmp/aborted-evidence.json",
            ),
        ];
        for (error, expected) in cases {
            let mut output = Vec::new();
            let result = run_calibration_with(|| Err(error), &mut output);
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains(expected));
            assert!(output.is_empty());
        }
    }

    #[test]
    fn calibration_error_reaches_top_level_stderr_and_nonzero_exit() {
        let mut stdout = Vec::new();
        let result = run_calibration_with(
            || {
                Err(loxa_core::calibration::CalibrationError::Provider(
                    loxa_core::provider::ProviderError::Unreachable,
                ))
            },
            &mut stdout,
        );
        let mut stderr = Vec::new();
        let exit = finish_cli_result(result, &mut stderr);
        assert_eq!(exit, ExitCode::from(1));
        assert!(stdout.is_empty());
        assert!(String::from_utf8(stderr)
            .unwrap()
            .contains("error: provider prerequisite failed: provider is unreachable"));
    }

    #[test]
    fn calibration_success_requires_absolute_evidence_for_every_verdict() {
        use loxa_core::evidence::EvidenceVerdict;
        use loxa_core::selector::SelectorVerdict;
        let verdicts = vec![
            (
                EvidenceVerdict::Selected {
                    schema_version: 1,
                    candidate_id: "gemma-3-4b-it-q4".into(),
                    reason_code: "only_managed_qualified".into(),
                },
                SelectorVerdict::Selected {
                    schema_version: 1,
                    candidate_id: "gemma-3-4b-it-q4".into(),
                    reason: "x".into(),
                },
            ),
            (
                EvidenceVerdict::NoVerifiedPlan {
                    schema_version: 1,
                    reason_codes: vec!["qualification_failed".into()],
                },
                SelectorVerdict::NoVerifiedPlan {
                    schema_version: 1,
                    reasons: vec!["x".into()],
                },
            ),
            (
                EvidenceVerdict::NoMaterialWinner {
                    schema_version: 1,
                    baseline_candidate_id: "gemma-3-4b-it-q4".into(),
                    reason_code: "no_material_winner".into(),
                },
                SelectorVerdict::NoMaterialWinner {
                    schema_version: 1,
                    baseline_candidate_id: "gemma-3-4b-it-q4".into(),
                    reason: "x".into(),
                },
            ),
        ];
        for (evidence_verdict, verdict) in verdicts {
            for path in [None, Some(PathBuf::from("relative.json"))] {
                let mut outcome =
                    calibration_outcome_for_test(evidence_verdict.clone(), verdict.clone());
                outcome.evidence_path = path;
                assert!(render_calibration_outcome(&outcome, &mut Vec::new()).is_err());
            }
        }
    }

    #[test]
    fn calibration_qualification_requires_all_five_clean_passes() {
        use loxa_core::workload::QualificationCaseResult;
        let mut outcome = calibration_outcome_for_test(
            loxa_core::evidence::EvidenceVerdict::NoVerifiedPlan {
                schema_version: 1,
                reason_codes: vec!["qualification_failed".into()],
            },
            loxa_core::selector::SelectorVerdict::NoVerifiedPlan {
                schema_version: 1,
                reasons: vec!["x".into()],
            },
        );
        outcome.evidence.qualifications[0].case_results = (0..4)
            .map(|i| QualificationCaseResult {
                schema_version: 1,
                case_id: format!("case-{i}"),
                passed: true,
                reason: None,
            })
            .collect();
        let mut output = Vec::new();
        render_calibration_outcome(&outcome, &mut output).unwrap();
        assert!(String::from_utf8(output)
            .unwrap()
            .contains("candidate A: failed — qualification_failed"));
    }

    #[test]
    fn calibration_unknown_or_empty_selected_candidate_is_an_error() {
        for id in ["", "unknown"] {
            let mut outcome = calibration_outcome_for_test(
                loxa_core::evidence::EvidenceVerdict::Selected {
                    schema_version: 1,
                    candidate_id: id.into(),
                    reason_code: "only_managed_qualified".into(),
                },
                loxa_core::selector::SelectorVerdict::Selected {
                    schema_version: 1,
                    candidate_id: id.into(),
                    reason: "x".into(),
                },
            );
            outcome.evidence.verdict = loxa_core::evidence::EvidenceVerdict::Selected {
                schema_version: 1,
                candidate_id: id.into(),
                reason_code: "only_managed_qualified".into(),
            };
            assert!(render_calibration_outcome(&outcome, &mut Vec::new()).is_err());
        }
    }

    fn calibration_outcome_for_test(
        evidence_verdict: loxa_core::evidence::EvidenceVerdict,
        verdict: loxa_core::selector::SelectorVerdict,
    ) -> loxa_core::calibration::CalibrationOutcome {
        use loxa_core::evidence::*;
        let mut a = loxa_core::provider::managed_llama::managed_candidate_spec(
            "fixture-provider",
            "fixture-revision",
        )
        .expect("valid fixture candidate");
        a.provider_kind = loxa_core::provider::ProviderKind::Ollama;
        a.ownership = loxa_core::provider::ProviderOwnership::Attached;
        a.endpoint = "http://127.0.0.1:11434".into();
        a.engine.engine_kind = "ollama-managed-gguf-engine".into();
        a.candidate_id = "gemma-3-4b-it-q4".into();
        let mut b = a.clone();
        b.candidate_id = "candidate-b".into();
        b.artifact.artifact_id = "candidate-b-artifact".into();
        let candidates = [
            CandidateEvidence {
                schema_version: 1,
                fingerprint: a.fingerprint(),
                identity: a,
            },
            CandidateEvidence {
                schema_version: 1,
                fingerprint: b.fingerprint(),
                identity: b,
            },
        ];
        loxa_core::calibration::CalibrationOutcome {
            evidence: CalibrationEvidence {
                schema_version: 1,
                protocol_version: CALIBRATION_PROTOCOL_VERSION.into(),
                workload_version: "tool-use-v1".into(),
                policy_version: "selector-v1".into(),
                started_at_unix_ms: 1,
                ended_at_unix_ms: 2,
                host: HostFingerprint {
                    schema_version: 1,
                    os_name: "test".into(),
                    os_version: "1".into(),
                    hardware_model: "test".into(),
                    physical_cores: 1,
                    logical_cores: 1,
                    memory_total_bytes: 1,
                    memory_available_bytes: 1,
                    root_disk_total_bytes: Some(1),
                    root_disk_available_bytes: Some(1),
                },
                qualifications: candidates
                    .iter()
                    .map(|candidate| QualificationEvidence {
                        schema_version: 1,
                        candidate_fingerprint: candidate.fingerprint.clone(),
                        case_results: (0..5)
                            .map(|i| loxa_core::workload::QualificationCaseResult {
                                schema_version: 1,
                                case_id: format!("case-{i}"),
                                passed: true,
                                reason: None,
                            })
                            .collect(),
                        failure_codes: vec![],
                    })
                    .collect(),
                candidates,
                disclosed_differences: vec![],
                measurements: vec![],
                isolation_observations: vec![],
                verdict: evidence_verdict,
                explanation_codes: vec![],
            },
            evidence_path: Some(std::env::temp_dir().join("calibration.json")),
            verdict,
        }
    }

    #[test]
    fn unknown_pull_id_renders_error_and_valid_ids() {
        let cli = Cli {
            command: Command::Pull {
                id: "missing-model".to_string(),
                quant: None,
            },
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        let exit = run(cli, &mut stdout, &mut stderr);

        assert_eq!(exit, std::process::ExitCode::from(1));
        assert!(stdout.is_empty());
        let stderr = String::from_utf8(stderr).expect("stderr is utf8");
        assert!(stderr.contains("unknown model id: missing-model"));
        assert!(stderr.contains("valid ids:"));
        for entry in REGISTRY {
            assert!(stderr.contains(entry.id));
        }
    }

    #[test]
    fn unknown_rm_id_renders_error_and_valid_ids() {
        let cli = Cli {
            command: Command::Rm {
                id: "missing-model".to_string(),
            },
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        let exit = run(cli, &mut stdout, &mut stderr);

        assert_eq!(exit, std::process::ExitCode::from(1));
        assert!(stdout.is_empty());
        let stderr = String::from_utf8(stderr).expect("stderr is utf8");
        assert!(stderr.contains("unknown model id: missing-model"));
        assert!(stderr.contains("valid ids:"));
        for entry in REGISTRY {
            assert!(stderr.contains(entry.id));
        }
    }

    #[test]
    fn unknown_run_id_renders_error_and_valid_ids() {
        let cli = Cli {
            command: Command::Run {
                id: "missing-model".to_string(),
                ctx: None,
                port: None,
                engine: RuntimeBackendKind::LlamaCpp,
            },
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let paths = NodePaths {
            models_dir: PathBuf::from("/tmp/unused-models"),
            state_path: PathBuf::from("/tmp/unused-managed.json"),
            logs_dir: PathBuf::from("/tmp/unused-logs"),
        };

        let exit = run_with_paths(cli, &paths, &mut stdout, &mut stderr);

        assert_eq!(exit, std::process::ExitCode::from(1));
        assert!(stdout.is_empty());
        let stderr = String::from_utf8(stderr).expect("stderr is utf8");
        assert!(stderr.contains("unknown model id: missing-model"));
        assert!(stderr.contains("valid ids:"));
        for entry in REGISTRY {
            assert!(stderr.contains(entry.id));
        }
    }

    #[test]
    fn model_not_downloaded_run_error_tells_user_to_pull() {
        let temp = TempDir::new("loxa-run-not-downloaded");
        let cli = Cli {
            command: Command::Run {
                id: "gemma-3-4b-it-q4".to_string(),
                ctx: None,
                port: None,
                engine: RuntimeBackendKind::LlamaCpp,
            },
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };

        let exit = run_with_paths(cli, &paths, &mut stdout, &mut stderr);

        assert_eq!(exit, std::process::ExitCode::from(1));
        assert!(stdout.is_empty());
        let stderr = String::from_utf8(stderr).expect("stderr is utf8");
        assert!(stderr.contains("model not downloaded"));
        assert!(stderr.contains("loxa pull gemma-3-4b-it-q4"));
    }

    #[test]
    fn invalid_python_model_fails_before_runtime_state_creation() {
        let temp = TempDir::new("loxa-python-invalid-model");
        let missing_model = temp.path().join("missing mlx model");
        let state_path = temp.path().join("managed.json");
        let cli = Cli {
            command: Command::Run {
                id: missing_model.display().to_string(),
                ctx: None,
                port: None,
                engine: RuntimeBackendKind::PyMlxLm,
            },
        };
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: state_path.clone(),
            logs_dir: temp.path().join("logs"),
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        let exit = run_with_paths(cli, &paths, &mut stdout, &mut stderr);

        assert_eq!(exit, ExitCode::from(1));
        assert!(stdout.is_empty());
        let stderr = String::from_utf8(stderr).expect("stderr is utf8");
        if cfg!(all(target_os = "macos", target_arch = "aarch64")) {
            assert!(stderr.contains("py-mlx-lm model path"), "{stderr}");
            assert!(stderr.contains("existing directory"), "{stderr}");
        } else {
            assert!(stderr.contains("requires Apple Silicon macOS"), "{stderr}");
        }
        assert!(!stderr.contains("unknown model id"), "{stderr}");
        assert!(
            !state_path.exists(),
            "validation must precede state creation"
        );
    }

    #[test]
    fn python_serve_without_a_model_passes_unloaded_node_validation() {
        let temp = TempDir::new("loxa-python-serve-no-model");
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };
        assert!(validate_cli_serve_request(None, RuntimeBackendKind::PyMlxLm, &paths).is_ok());
        assert!(!paths.state_path.exists());
    }

    #[test]
    fn ps_renders_clear_message_when_no_sidecars_exist() {
        let temp = TempDir::new("loxa-ps-empty");
        let cli = Cli {
            command: Command::Ps,
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };

        let exit = run_with_paths(cli, &paths, &mut stdout, &mut stderr);

        assert_eq!(exit, std::process::ExitCode::SUCCESS);
        assert!(stderr.is_empty());
        let stdout = String::from_utf8(stdout).expect("stdout is utf8");
        assert!(stdout.contains("no managed sidecars"));
    }

    #[test]
    fn ps_legacy_sentinel_without_managed_json_fails_closed_with_exact_recovery_guidance() {
        let temp = TempDir::new("loxa-ps-legacy-sentinel");
        let state_path = temp.path().join("managed.json");
        let sentinel_path = state_path.with_file_name("managed.json.lock");
        fs::create_dir_all(temp.path()).expect("create temp root");
        fs::write(&sentinel_path, b"legacy owner metadata\n").expect("write legacy sentinel");
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: state_path.clone(),
            logs_dir: temp.path().join("logs"),
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        let exit = run_with_paths(
            Cli {
                command: Command::Ps,
            },
            &paths,
            &mut stdout,
            &mut stderr,
        );

        assert_eq!(exit, std::process::ExitCode::SUCCESS);
        assert!(stderr.is_empty());
        assert_eq!(
            String::from_utf8(stdout).expect("stdout is utf8"),
            format!(
                "legacy managed sidecar state requires manual recovery at {}; confirm no old Loxa process remains, then archive it manually\n",
                sentinel_path.display()
            )
        );
        assert!(sentinel_path.exists());
        assert!(!state_path.exists());
        assert!(!state_path.with_file_name("managed.json.v2.lock").exists());
    }

    #[test]
    fn ps_renders_childless_live_owner_as_starting() {
        let temp = TempDir::new("loxa-ps-starting");
        let state_path = temp.path().join("managed.json");
        let mut run = starting_run_for_test(&state_path, "run-starting");
        set_test_owner_to_current_process(&mut run);
        persist_test_run(&state_path, run);

        let stdout = render_ps_for_test(&temp);
        let fields = only_ps_row_fields(&stdout);

        assert_eq!(fields[1], "-");
        assert_eq!(fields[3], "starting");
    }

    #[test]
    fn ps_renders_live_owner_with_stop_intent_as_stopping() {
        let temp = TempDir::new("loxa-ps-stopping");
        let state_path = temp.path().join("managed.json");
        let mut run = starting_run_for_test(&state_path, "run-stopping");
        set_test_owner_to_current_process(&mut run);
        run.stop_requested = true;
        run.lifecycle = supervisor::RunLifecycle::Running;
        run.child_pid = Some(u32::MAX);
        run.child_process_start_time_unix_s = Some(1);
        persist_test_run(&state_path, run);

        let stdout = render_ps_for_test(&temp);
        let fields = only_ps_row_fields(&stdout);

        assert_eq!(fields[3], "stopping");
    }

    #[test]
    fn ps_renders_dead_owner_with_live_child_as_recovery_required() {
        let temp = TempDir::new("loxa-ps-dead-owner");
        let state_path = temp.path().join("managed.json");
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind live child port");
        let mut run = starting_run_for_test(&state_path, "run-dead-owner");
        run.owner_pid = u32::MAX;
        run.owner_process_start_time_unix_s = 1;
        set_test_child_to_current_process(&mut run, &listener);
        persist_test_run(&state_path, run);

        let stdout = render_ps_for_test(&temp);
        let fields = only_ps_row_fields(&stdout);

        assert_eq!(fields[1], std::process::id().to_string());
        assert_eq!(fields[3], "recovery-required");
        assert!(!stdout.contains("  running"));
    }

    #[test]
    fn ps_renders_running_only_for_live_owner_and_exact_live_child() {
        let temp = TempDir::new("loxa-ps-running");
        let state_path = temp.path().join("managed.json");
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind live child port");
        let mut run = starting_run_for_test(&state_path, "run-running");
        set_test_owner_to_current_process(&mut run);
        set_test_child_to_current_process(&mut run, &listener);
        persist_test_run(&state_path, run);

        let stdout = render_ps_for_test(&temp);
        let fields = only_ps_row_fields(&stdout);

        assert_eq!(fields[1], std::process::id().to_string());
        assert_eq!(fields[3], "running");
    }

    #[test]
    fn ps_model_column_renders_the_canonical_model_path_without_shifting_child_pid() {
        let temp = TempDir::new("loxa-ps-model-column");
        let state_path = temp.path().join("managed.json");
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind live child port");
        let mut run = starting_run_for_test(&state_path, "run-model-column");
        set_test_owner_to_current_process(&mut run);
        set_test_child_to_current_process(&mut run, &listener);
        persist_test_run(&state_path, run);

        let stdout = render_ps_for_test(&temp);
        let fields = only_ps_row_fields(&stdout);
        let entry = registry::find("gemma-3-4b-it-q4").expect("registry entry");
        let expected_model_path = temp.path().join("models").join(entry.filename);

        assert_eq!(fields[1], std::process::id().to_string());
        assert_eq!(fields[3], "running");
        assert_eq!(fields[4], expected_model_path.display().to_string());
    }

    #[test]
    fn ps_marks_inconsistent_entries_as_recovery_required() {
        let temp = TempDir::new("loxa-ps-stale");
        let cli = Cli {
            command: Command::Ps,
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let stale = loxa_core::supervisor::ManagedServer {
            id: "gemma-3-4b-it-q4".to_string(),
            pid: 999_999,
            port: 65_530,
            model_path: temp.path().join("model.gguf"),
            started_at_unix_s: 1_700_000_000,
            llama_server_version: "test".to_string(),
            process_start_time_unix_s: Some(1),
        };
        persist_run_for_server(&temp.path().join("managed.json"), &stale);
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };

        let exit = run_with_paths(cli, &paths, &mut stdout, &mut stderr);

        assert_eq!(exit, std::process::ExitCode::SUCCESS);
        assert!(stderr.is_empty());
        let stdout = String::from_utf8(stdout).expect("stdout is utf8");
        assert!(stdout.contains("recovery-required"));
        assert!(stdout.contains("gemma-3-4b-it-q4"));
    }

    #[test]
    fn ps_reports_corrupt_state_without_failing() {
        let temp = TempDir::new("loxa-ps-corrupt");
        fs::create_dir_all(temp.path()).expect("create temp root");
        fs::write(temp.path().join("managed.json"), "{not-json").expect("write corrupt state");
        let cli = Cli {
            command: Command::Ps,
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };

        let exit = run_with_paths(cli, &paths, &mut stdout, &mut stderr);

        assert_eq!(exit, std::process::ExitCode::SUCCESS);
        assert!(stderr.is_empty());
        let stdout = String::from_utf8(stdout).expect("stdout is utf8");
        assert!(stdout.contains("managed sidecar state is corrupt"));
    }

    #[test]
    fn run_reports_corrupt_state_to_stderr_and_exits_1() {
        let temp = TempDir::new("loxa-run-corrupt");
        fs::create_dir_all(temp.path()).expect("create temp root");
        fs::write(temp.path().join("managed.json"), "{not-json").expect("write corrupt state");
        let cli = Cli {
            command: Command::Run {
                id: "gemma-3-4b-it-q4".to_string(),
                ctx: None,
                port: None,
                engine: RuntimeBackendKind::LlamaCpp,
            },
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };

        let exit = run_with_paths(cli, &paths, &mut stdout, &mut stderr);

        assert_eq!(exit, std::process::ExitCode::from(1));
        assert!(stdout.is_empty());
        let stderr = String::from_utf8(stderr).expect("stderr is utf8");
        assert!(stderr.contains("managed sidecar state is corrupt"));
    }

    #[test]
    fn stop_all_is_idempotent_when_no_sidecars_exist() {
        let temp = TempDir::new("loxa-stop-all-empty");
        let cli = Cli {
            command: Command::Stop {
                target: "all".to_string(),
            },
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };

        let exit = run_with_paths(cli, &paths, &mut stdout, &mut stderr);

        assert_eq!(exit, std::process::ExitCode::SUCCESS);
        assert!(stderr.is_empty());
        let stdout = String::from_utf8(stdout).expect("stdout is utf8");
        assert!(stdout.contains("no managed sidecars"));
    }

    #[test]
    fn stop_all_reports_corrupt_state_to_stderr_and_exits_1() {
        let temp = TempDir::new("loxa-stop-all-corrupt");
        fs::create_dir_all(temp.path()).expect("create temp root");
        fs::write(temp.path().join("managed.json"), "{not-json").expect("write corrupt state");
        let cli = Cli {
            command: Command::Stop {
                target: "all".to_string(),
            },
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: temp.path().join("managed.json"),
            logs_dir: temp.path().join("logs"),
        };

        let exit = run_with_paths(cli, &paths, &mut stdout, &mut stderr);

        assert_eq!(exit, std::process::ExitCode::from(1));
        assert!(stdout.is_empty());
        let stderr = String::from_utf8(stderr).expect("stderr is utf8");
        assert!(stderr.contains("managed sidecar state is corrupt"));
    }

    #[test]
    fn cli_stop_dead_owner_records_durable_intent_and_preserves_full_run() {
        let temp = TempDir::new("loxa-stop-dead-owner");
        let state_path = temp.path().join("managed.json");
        let mut run = starting_run_for_test(&state_path, "run-1");
        run.owner_pid = 999_999;
        run.owner_process_start_time_unix_s = 1;
        supervisor::create_starting_run(&state_path, run.clone()).expect("create run");
        let starting_identity = run.identity();
        run.lifecycle = supervisor::RunLifecycle::Running;
        run.child_pid = Some(999_998);
        run.child_process_start_time_unix_s = Some(2);
        run.child_pgid = Some(999_998);
        let run =
            supervisor::update_runtime_state_run_committed(&state_path, &starting_identity, run)
                .expect("attach child metadata")
                .expect("exact attachment");
        let paths = NodePaths {
            models_dir: temp.path().join("models"),
            state_path: state_path.clone(),
            logs_dir: temp.path().join("logs"),
        };
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        let exit = render_stop_outcome(
            run.model_id.as_deref().unwrap(),
            stop_managed_servers(
                StopRequest {
                    target: run.model_id.as_deref().unwrap(),
                },
                &paths,
            ),
            &mut stdout,
            &mut stderr,
        )
        .expect("stop command result");

        assert_eq!(exit, ExitCode::from(1));
        assert!(stdout.is_empty());
        assert!(String::from_utf8(stderr)
            .expect("stderr utf8")
            .contains("recovery required"));
        let RuntimeStateRead::Loaded(runs) =
            supervisor::read_runtime_state(&state_path).expect("read preserved run")
        else {
            panic!("expected loaded run");
        };
        assert_eq!(runs.len(), 1);
        let mut expected = run;
        expected.stop_requested = true;
        assert_eq!(runs[0], expected);
    }

    #[test]
    fn model_status_prioritizes_downloaded_then_partial_then_not_downloaded() {
        let temp = TempDir::new("loxa-status");
        let entry = &REGISTRY[0];
        let (final_path, part_path) = model_paths(entry, temp.path());

        assert_eq!(model_status(entry, temp.path()), ModelStatus::NotDownloaded);

        fs::write(&part_path, b"partial").expect("write part file");
        assert_eq!(model_status(entry, temp.path()), ModelStatus::Partial);

        fs::write(&final_path, b"final").expect("write final file");
        assert_eq!(model_status(entry, temp.path()), ModelStatus::Downloaded);
    }

    #[test]
    fn remove_model_files_deletes_final_and_part_then_returns_empty_when_absent() {
        let temp = TempDir::new("loxa-rm");
        let entry = &REGISTRY[0];
        let (final_path, part_path) = model_paths(entry, temp.path());
        fs::write(&final_path, b"final").expect("write final file");
        fs::write(&part_path, b"partial").expect("write part file");

        let removed = remove_model_files(entry, temp.path()).expect("remove model files");

        assert_eq!(removed, vec![final_path.clone(), part_path.clone()]);
        assert!(!final_path.exists());
        assert!(!part_path.exists());

        let removed = remove_model_files(entry, temp.path()).expect("remove absent model files");
        assert!(removed.is_empty());
    }

    #[test]
    fn remove_user_entry_deletes_registry_final_and_partial_files() {
        let temp = TempDir::new("loxa-user-rm");
        let registry_dir = temp.path().join("registry.d");
        let models_dir = temp.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        let entry = registry::UserModelEntry {
            id: "demo-q4-k-m".into(),
            repo: "owner/repo".into(),
            revision: "0123456789abcdef0123456789abcdef01234567".into(),
            filename: "demo-Q4_K_M.gguf".into(),
            sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            size_bytes: 100 * 1024 * 1024,
            license: "apache-2.0".into(),
            params: "unknown".into(),
            quant: "Q4_K_M".into(),
            min_free_mem_gb: 0.1,
        };
        let registry_path = registry::save_user_entry(&registry_dir, &entry).unwrap();
        let final_path = models_dir.join(&entry.filename);
        let part_path = models_dir.join(format!("{}.part", entry.filename));
        fs::write(&final_path, b"final").unwrap();
        fs::write(&part_path, b"partial").unwrap();

        let removed = remove_user_entry(&entry.id, &registry_dir, &models_dir)
            .unwrap()
            .unwrap();

        assert_eq!(
            removed,
            vec![final_path.clone(), part_path.clone(), registry_path.clone()]
        );
        assert!(!final_path.exists() && !part_path.exists() && !registry_path.exists());
    }

    #[test]
    fn bytes_to_gb_string_uses_one_decimal() {
        assert_eq!(bytes_to_gb_string(0), "0.0");
        assert_eq!(bytes_to_gb_string(1_073_741_824), "1.0");
        assert_eq!(bytes_to_gb_string(1_610_612_736), "1.5");
    }

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(prefix: &str) -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time after unix epoch")
                .as_nanos();
            let path =
                std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()));
            fs::create_dir_all(&path).expect("create temp dir");
            Self { path }
        }

        fn path(&self) -> &std::path::Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}
