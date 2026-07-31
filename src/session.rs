use crate::chat::{Event, Message, Role, Worker};
use crate::runner::ForegroundServer;
use anstyle::{AnsiColor, Style};
use inquire::error::{CustomUserError, InquireError};
use inquire::Text;
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

fn autocomplete_slash(input: &str) -> Result<Vec<String>, CustomUserError> {
    Ok(slash_suggestions(input)
        .into_iter()
        .map(str::to_owned)
        .collect())
}

fn color(color: AnsiColor) -> Style {
    Style::new().fg_color(Some(color.into()))
}

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

fn prompt_input() -> InputEvent {
    match Text::new("You")
        .with_placeholder("message or / for commands")
        .with_autocomplete(autocomplete_slash)
        .prompt()
    {
        Ok(line) => InputEvent::Line(line),
        Err(InquireError::OperationCanceled) => InputEvent::Canceled,
        Err(InquireError::OperationInterrupted) => InputEvent::Interrupted,
        Err(error) => InputEvent::Error(error.to_string()),
    }
}

pub(crate) fn run(mut server: ForegroundServer, model: &str) -> Result<i32, String> {
    let mut session = Session::default();
    let ready = color(AnsiColor::Green).bold();
    let model_style = color(AnsiColor::Cyan).bold();
    let dim = Style::new().dimmed();
    anstream::println!("{ready}Ready{ready:#} · {model_style}{model}{model_style:#}");
    anstream::println!("{dim}Type / for commands · Esc or Ctrl-C to exit{dim:#}");

    loop {
        if let Some(code) = server.poll()? {
            return Ok(code);
        }
        let event = prompt_input();

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
                let heading = color(AnsiColor::Cyan).bold();
                anstream::println!("{heading}Available commands{heading:#}");
                for (command, description) in SLASH_COMMANDS {
                    anstream::println!("  {heading}{command:<7}{heading:#} {description}");
                }
                continue;
            }
            InputAction::Clear => {
                session.clear();
                let success = color(AnsiColor::Green);
                anstream::println!("{success}Conversation cleared.{success:#}");
                continue;
            }
            InputAction::Exit => {
                server.terminate()?;
                return Ok(0);
            }
            InputAction::Reject(error) => {
                let error_style = color(AnsiColor::Red).bold();
                anstream::eprintln!(
                    "{error_style}Unknown command:{error_style:#} {error}. Type /help for commands."
                );
                continue;
            }
            InputAction::Prompt(user) => user,
        };

        if let Some(code) = server.poll()? {
            return Ok(code);
        }

        let worker = Worker::start(server.port(), model.to_owned(), session.request(&user))?;
        let assistant = color(AnsiColor::Magenta).bold();
        anstream::print!("{assistant}Loxa{assistant:#} ");
        std::io::stdout()
            .flush()
            .map_err(|error| error.to_string())?;
        let result = loop {
            match server.poll() {
                Ok(Some(code)) => {
                    worker.join()?;
                    return Ok(code);
                }
                Ok(None) => {}
                Err(error) => {
                    let cleanup = server.terminate();
                    worker.join()?;
                    cleanup?;
                    return Err(error);
                }
            }
            match worker.recv_timeout(POLL_INTERVAL) {
                Ok(Event::Delta(delta)) => {
                    print!("{delta}");
                    std::io::stdout()
                        .flush()
                        .map_err(|error| error.to_string())?;
                }
                Ok(Event::Complete(assistant)) => break Ok(assistant),
                Ok(Event::Error(error)) => break Err(error),
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    break Err("chat worker stopped before completion".into())
                }
            }
        };
        println!();
        worker.join()?;
        match result {
            Ok(assistant) => session.complete(user, assistant),
            Err(error) => {
                let error_style = color(AnsiColor::Red).bold();
                anstream::eprintln!("{error_style}Error:{error_style:#} {error}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{slash_suggestions, InputAction, Session};
    use crate::chat::{Message, Role};

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
}
