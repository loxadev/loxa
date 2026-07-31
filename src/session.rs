use crate::chat::{Event, Message, Role, Worker};
use crate::runner::ForegroundServer;
use std::io::{BufRead, Write};
use std::sync::mpsc;
use std::time::Duration;

const POLL_INTERVAL: Duration = Duration::from_millis(20);

#[derive(Debug, Eq, PartialEq)]
enum InputAction {
    Ignore,
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
            "/clear" => InputAction::Clear,
            "/exit" => InputAction::Exit,
            command if command.starts_with('/') => {
                InputAction::Reject(format!("unknown command {command}"))
            }
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
    Eof,
    Error(String),
}

fn input_events() -> Result<mpsc::Receiver<InputEvent>, String> {
    let (sender, receiver) = mpsc::sync_channel(1);
    std::thread::Builder::new()
        .name("loxa-terminal-input".into())
        .spawn(move || {
            let stdin = std::io::stdin();
            let mut input = stdin.lock();
            loop {
                let mut line = String::new();
                match input.read_line(&mut line) {
                    Ok(0) => {
                        let _ = sender.send(InputEvent::Eof);
                        return;
                    }
                    Ok(_) => {
                        if sender.send(InputEvent::Line(line)).is_err() {
                            return;
                        }
                    }
                    Err(error) => {
                        let _ = sender.send(InputEvent::Error(error.to_string()));
                        return;
                    }
                }
            }
        })
        .map_err(|error| error.to_string())?;
    Ok(receiver)
}

pub(crate) fn run(mut server: ForegroundServer, model: &str) -> Result<i32, String> {
    let input = input_events()?;
    let mut session = Session::default();
    println!("Chat ready. Use /clear to reset or /exit to quit.");

    loop {
        print!("> ");
        std::io::stdout()
            .flush()
            .map_err(|error| error.to_string())?;
        let event = loop {
            if let Some(code) = server.poll()? {
                return Ok(code);
            }
            match input.recv_timeout(POLL_INTERVAL) {
                Ok(event) => break event,
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => break InputEvent::Eof,
            }
        };

        let action = match event {
            InputEvent::Line(line) => Session::classify(Some(&line)),
            InputEvent::Eof => {
                println!();
                Session::classify(None)
            }
            InputEvent::Error(error) => {
                server.terminate()?;
                return Err(format!("failed to read terminal input: {error}"));
            }
        };
        let user = match action {
            InputAction::Ignore => continue,
            InputAction::Clear => {
                session.clear();
                println!("History cleared.");
                continue;
            }
            InputAction::Exit => {
                server.terminate()?;
                return Ok(0);
            }
            InputAction::Reject(error) => {
                eprintln!("{error}");
                continue;
            }
            InputAction::Prompt(user) => user,
        };

        let worker = Worker::start(server.port(), model.to_owned(), session.request(&user))?;
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
            Err(error) => eprintln!("error: {error}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{InputAction, Session};
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
        assert_eq!(Session::classify(None), InputAction::Exit);
        assert_eq!(
            Session::classify(Some("/unknown\n")),
            InputAction::Reject("unknown command /unknown".into())
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
}
