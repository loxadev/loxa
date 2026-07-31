use crate::chat::{Event, Message, Role, Worker};
use crate::runner::{report_exit, ForegroundServer};
use crate::ui;
use rustyline::completion::Completer;
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::{Hint, Hinter};
use rustyline::history::DefaultHistory;
use rustyline::validate::{ValidationContext, ValidationResult, Validator};
use rustyline::{CompletionType, Config, Context, Editor, Helper};
use std::io::Write;
use std::sync::mpsc;
use std::time::Duration;

const POLL_INTERVAL: Duration = Duration::from_millis(20);
const SLASH_COMMANDS: [(&str, &str); 3] = [
    ("/clear", "Clear conversation history"),
    ("/help", "Show available commands"),
    ("/exit", "Exit chat"),
];

fn slash_suggestions(input: &str) -> Vec<&'static str> {
    if !input.starts_with('/') {
        return Vec::new();
    }
    SLASH_COMMANDS
        .iter()
        .map(|(command, _)| *command)
        .filter(|command| command.starts_with(input))
        .collect()
}

struct ChatHelper;

struct CommandHint;

impl Hint for CommandHint {
    fn display(&self) -> &str {
        "  /clear  /help  /exit"
    }

    fn completion(&self) -> Option<&str> {
        None
    }
}

impl Completer for ChatHelper {
    type Candidate = String;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _context: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Self::Candidate>)> {
        let input = &line[..pos];
        Ok((
            0,
            slash_suggestions(input)
                .into_iter()
                .map(str::to_owned)
                .collect(),
        ))
    }
}

impl Hinter for ChatHelper {
    type Hint = CommandHint;

    fn hint(&self, line: &str, pos: usize, _context: &Context<'_>) -> Option<Self::Hint> {
        (line == "/" && pos == line.len()).then_some(CommandHint)
    }
}

impl Highlighter for ChatHelper {}
impl Validator for ChatHelper {
    fn validate(&self, context: &mut ValidationContext<'_>) -> rustyline::Result<ValidationResult> {
        if continues_on_next_line(context.input()) {
            Ok(ValidationResult::Incomplete)
        } else {
            Ok(ValidationResult::Valid(None))
        }
    }
}
impl Helper for ChatHelper {}

type ChatEditor = Editor<ChatHelper, DefaultHistory>;

#[derive(Debug, Eq, PartialEq)]
enum InputAction {
    Ignore,
    Help,
    Clear,
    Exit,
    Prompt(String),
    Reject(String),
}

#[derive(Default)]
struct Session {
    history: Vec<Message>,
}

impl Session {
    fn classify(line: Option<&str>) -> InputAction {
        let Some(line) = line else {
            return InputAction::Exit;
        };
        let input = line.trim();
        match input {
            "" => InputAction::Ignore,
            "/" | "/?" | "/help" => InputAction::Help,
            "/clear" => InputAction::Clear,
            "/exit" => InputAction::Exit,
            command if command.starts_with('/') => InputAction::Reject(command.to_owned()),
            prompt => InputAction::Prompt(prompt.to_owned()),
        }
    }

    fn request(&self, user: &str) -> Vec<Message> {
        let mut messages = self.history.clone();
        messages.push(Message {
            role: Role::User,
            content: user.to_owned(),
        });
        messages
    }

    fn complete(&mut self, user: String, assistant: String) {
        self.history.push(Message {
            role: Role::User,
            content: user,
        });
        self.history.push(Message {
            role: Role::Assistant,
            content: assistant,
        });
    }

    fn clear(&mut self) {
        self.history.clear();
    }
}

enum InputEvent {
    Line(String),
    Canceled,
    Interrupted,
    Error(String),
}

fn chat_config() -> Config {
    Config::builder()
        .completion_type(CompletionType::List)
        .completion_show_all_if_ambiguous(true)
        .bracketed_paste(true)
        .build()
}

fn new_editor() -> Result<ChatEditor, String> {
    let mut editor = Editor::with_config(chat_config()).map_err(|error| error.to_string())?;
    editor.set_helper(Some(ChatHelper));
    Ok(editor)
}

fn continues_on_next_line(input: &str) -> bool {
    input
        .chars()
        .rev()
        .take_while(|character| *character == '\\')
        .count()
        % 2
        == 1
}

fn remove_line_continuations(input: String) -> String {
    input.replace("\\\n", "\n")
}

fn prompt_input(editor: &mut ChatEditor) -> InputEvent {
    match editor.readline("> ") {
        Ok(line) => {
            let line = remove_line_continuations(line);
            if !line.trim().is_empty() {
                if let Err(error) = editor.add_history_entry(line.as_str()) {
                    return InputEvent::Error(error.to_string());
                }
            }
            InputEvent::Line(line)
        }
        Err(ReadlineError::Eof) => InputEvent::Canceled,
        Err(ReadlineError::Interrupted) => InputEvent::Interrupted,
        Err(error) => InputEvent::Error(error.to_string()),
    }
}

fn write_assistant_delta(output: &mut impl Write, delta: &str) -> Result<(), String> {
    output
        .write_all(delta.as_bytes())
        .and_then(|_| output.flush())
        .map_err(|error| error.to_string())
}

pub(crate) fn run(mut server: ForegroundServer, model: &str) -> Result<i32, String> {
    let mut session = Session::default();
    let mut editor = new_editor()?;
    let ready = ui::success();
    let model_style = ui::accent();
    let dim = ui::muted();
    anstream::println!("{ready}Ready{ready:#} · {model_style}{model}{model_style:#}");
    anstream::println!(
        "{dim}Enter / for commands · \\ then Enter for a new line · Ctrl-D or Ctrl-C to exit{dim:#}"
    );

    loop {
        if let Some(exit) = server.poll()? {
            return Ok(report_exit(exit));
        }
        let event = prompt_input(&mut editor);

        let action = match event {
            InputEvent::Line(line) => Session::classify(Some(&line)),
            InputEvent::Canceled => {
                println!();
                Session::classify(None)
            }
            InputEvent::Interrupted => {
                server.terminate()?;
                return Ok(130);
            }
            InputEvent::Error(error) => {
                server.terminate()?;
                return Err(format!("failed to read terminal input: {error}"));
            }
        };
        let user = match action {
            InputAction::Ignore => continue,
            InputAction::Help => {
                let heading = ui::accent();
                anstream::println!("{heading}Available commands{heading:#}");
                for (command, description) in SLASH_COMMANDS {
                    anstream::println!("  {heading}{command:<7}{heading:#} {description}");
                }
                continue;
            }
            InputAction::Clear => {
                session.clear();
                let success = ui::success();
                anstream::println!("{success}Conversation cleared.{success:#}");
                continue;
            }
            InputAction::Exit => {
                server.terminate()?;
                return Ok(0);
            }
            InputAction::Reject(error) => {
                let error_style = ui::danger();
                anstream::eprintln!(
                    "{error_style}Unknown command:{error_style:#} {error}. Type /help for commands."
                );
                continue;
            }
            InputAction::Prompt(user) => user,
        };

        if let Some(exit) = server.poll()? {
            return Ok(report_exit(exit));
        }

        let worker = Worker::start(server.port(), model.to_owned(), session.request(&user))?;
        let thinking = ui::spinner("Thinking".into());
        let mut waiting = true;
        let mut output = std::io::stdout();
        let result = loop {
            match server.poll() {
                Ok(Some(exit)) => {
                    thinking.finish_and_clear();
                    worker.join()?;
                    return Ok(report_exit(exit));
                }
                Ok(None) => {}
                Err(error) => {
                    thinking.finish_and_clear();
                    let cleanup = server.terminate();
                    worker.join()?;
                    cleanup?;
                    return Err(error);
                }
            }
            match worker.recv_timeout(POLL_INTERVAL) {
                Ok(Event::Delta(delta)) => {
                    if waiting {
                        thinking.finish_and_clear();
                        waiting = false;
                    }
                    write_assistant_delta(&mut output, &delta)?;
                }
                Ok(Event::Complete(assistant)) => {
                    thinking.finish_and_clear();
                    break Ok(assistant);
                }
                Ok(Event::Error(error)) => {
                    thinking.finish_and_clear();
                    break Err(error);
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    thinking.finish_and_clear();
                    break Err("chat worker stopped before completion".into());
                }
            }
        };
        writeln!(output).map_err(|error| error.to_string())?;
        worker.join()?;
        match result {
            Ok(assistant) => session.complete(user, assistant),
            Err(error) => {
                let error_style = ui::danger();
                anstream::eprintln!("{error_style}Error:{error_style:#} {error}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        chat_config, continues_on_next_line, remove_line_continuations, slash_suggestions,
        write_assistant_delta, ChatHelper, InputAction, Session,
    };
    use crate::chat::{Message, Role};
    use rustyline::completion::Completer as _;
    use rustyline::history::{DefaultHistory, History};
    use rustyline::Context;

    #[test]
    fn two_successful_turns_are_sent_in_order() {
        let mut session = Session::default();
        assert_eq!(
            session.request("first"),
            vec![Message {
                role: Role::User,
                content: "first".into()
            }]
        );
        session.complete("first".into(), "one".into());
        assert_eq!(
            session.request("second"),
            vec![
                Message {
                    role: Role::User,
                    content: "first".into()
                },
                Message {
                    role: Role::Assistant,
                    content: "one".into()
                },
                Message {
                    role: Role::User,
                    content: "second".into()
                },
            ]
        );
    }

    #[test]
    fn blank_commands_and_regular_prompts_are_classified_locally() {
        assert_eq!(Session::classify(Some(" \n")), InputAction::Ignore);
        assert_eq!(Session::classify(Some("/clear\n")), InputAction::Clear);
        assert_eq!(Session::classify(Some("/exit\n")), InputAction::Exit);
        assert_eq!(Session::classify(Some("/\n")), InputAction::Help);
        assert_eq!(Session::classify(Some("/?\n")), InputAction::Help);
        assert_eq!(Session::classify(Some("/help\n")), InputAction::Help);
        assert_eq!(Session::classify(None), InputAction::Exit);
        assert_eq!(
            Session::classify(Some("/unknown\n")),
            InputAction::Reject("/unknown".into())
        );
        assert_eq!(
            Session::classify(Some(" hello \n")),
            InputAction::Prompt("hello".into())
        );
    }

    #[test]
    fn failed_turn_does_not_change_history_and_clear_removes_it() {
        let mut session = Session::default();
        session.complete("kept".into(), "answer".into());
        let before = session.history.clone();
        let _failed_request = session.request("not committed");
        assert_eq!(session.history, before);
        session.clear();
        assert!(session.history.is_empty());
    }

    #[test]
    fn slash_prefix_shows_matching_commands() {
        assert_eq!(slash_suggestions("/"), ["/clear", "/help", "/exit"]);
        assert_eq!(slash_suggestions("/c"), ["/clear"]);
        assert!(slash_suggestions("hello").is_empty());
    }

    #[test]
    fn chat_editor_completes_slash_commands() {
        let history = DefaultHistory::new();
        let context = Context::new(&history);

        let (start, candidates) = ChatHelper.complete("/c", 2, &context).unwrap();

        assert_eq!(start, 0);
        assert_eq!(candidates, ["/clear"]);
    }

    #[test]
    fn typing_slash_displays_commands_without_inserting_the_menu() {
        use rustyline::hint::{Hint as _, Hinter as _};

        let history = DefaultHistory::new();
        let context = Context::new(&history);

        let hint = ChatHelper.hint("/", 1, &context).unwrap();

        assert_eq!(hint.display(), "  /clear  /help  /exit");
        assert_eq!(hint.completion(), None);
    }

    #[test]
    fn chat_editor_keeps_session_history_and_accepts_bracketed_paste() {
        let mut editor = super::new_editor().unwrap();

        editor.add_history_entry("first").unwrap();
        editor.add_history_entry("second").unwrap();

        assert_eq!(editor.history().len(), 2);
        assert!(editor
            .history()
            .get(0, rustyline::history::SearchDirection::Forward)
            .is_ok());
        assert!(editor
            .history()
            .get(1, rustyline::history::SearchDirection::Forward)
            .is_ok());

        let config = chat_config();
        assert!(config.enable_bracketed_paste());
        assert!(config.completion_show_all_if_ambiguous());
    }

    #[test]
    fn trailing_unescaped_backslash_creates_a_multiline_prompt() {
        assert!(continues_on_next_line("first\\"));
        assert!(!continues_on_next_line("first\\\\"));
        assert!(!continues_on_next_line("first"));
        assert_eq!(
            remove_line_continuations("first\\\nsecond".into()),
            "first\nsecond"
        );
    }

    #[test]
    fn assistant_deltas_are_written_without_a_speaker_label() {
        let mut output = Vec::new();

        write_assistant_delta(&mut output, "hello").unwrap();
        write_assistant_delta(&mut output, " there").unwrap();

        assert_eq!(output, b"hello there");
    }
}
