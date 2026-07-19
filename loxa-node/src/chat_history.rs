use loxa_core::chat_history::{
    ChatCursor, ChatHistoryRepository, ChatId, ChatSummary, HistoryError, HistoryPage,
    MessageContent, MessageId, MessagePage, MessageSummary, Title, TurnCursor, TurnId, TurnMetrics,
    TurnPage, TurnProvenance, TurnRecord, TurnState,
};
use std::path::PathBuf;
use std::sync::mpsc::{self, Sender, SyncSender, TrySendError};
use std::thread::{self, JoinHandle};
use tokio::sync::oneshot;

const HISTORY_QUEUE_CAPACITY: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChatHistoryError {
    Busy,
    Stopped,
    Repository(HistoryError),
}

enum Command {
    Create(i64, oneshot::Sender<Result<ChatSummary, HistoryError>>),
    Get(ChatId, oneshot::Sender<Result<ChatSummary, HistoryError>>),
    List(
        usize,
        Option<ChatCursor>,
        oneshot::Sender<Result<HistoryPage, HistoryError>>,
    ),
    Rename(
        ChatId,
        Title,
        i64,
        oneshot::Sender<Result<ChatSummary, HistoryError>>,
    ),
    Delete(ChatId, oneshot::Sender<Result<(), HistoryError>>),
    Clear(oneshot::Sender<Result<usize, HistoryError>>),
    Turns(
        ChatId,
        usize,
        Option<TurnCursor>,
        oneshot::Sender<Result<TurnPage, HistoryError>>,
    ),
    Messages(
        TurnId,
        oneshot::Sender<Result<Vec<MessageSummary>, HistoryError>>,
    ),
    GetTurn(TurnId, oneshot::Sender<Result<TurnRecord, HistoryError>>),
    MessagePage(
        MessageId,
        u32,
        oneshot::Sender<Result<MessagePage, HistoryError>>,
    ),
    Begin(
        ChatId,
        MessageContent,
        TurnProvenance,
        i64,
        oneshot::Sender<Result<TurnRecord, HistoryError>>,
    ),
    Checkpoint(
        TurnId,
        MessageContent,
        i64,
        oneshot::Sender<Result<(), HistoryError>>,
    ),
    #[cfg(test)]
    FailNextFinalize(oneshot::Sender<()>),
    Stop,
}

struct TerminalCommand {
    turn: TurnId,
    state: TurnState,
    content: MessageContent,
    code: Option<String>,
    metrics: TurnMetrics,
    at: i64,
    response: oneshot::Sender<Result<(), HistoryError>>,
}

#[derive(Clone)]
pub struct ChatHistory {
    sender: SyncSender<Command>,
    terminal_sender: Sender<TerminalCommand>,
}

pub struct ChatHistoryWorker {
    sender: SyncSender<Command>,
    terminal_sender: Sender<TerminalCommand>,
    join: Option<JoinHandle<Result<(), HistoryError>>>,
    completion: mpsc::Receiver<()>,
}

struct HistoryCompletion(mpsc::SyncSender<()>);

impl Drop for HistoryCompletion {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

pub(crate) enum ChatHistoryShutdownResult {
    Stopped,
    Failed(HistoryError),
    Retained(ChatHistoryShutdownFailure),
}

#[must_use = "history shutdown failure retains worker ownership"]
pub(crate) struct ChatHistoryShutdownFailure {
    _worker: std::mem::ManuallyDrop<ChatHistoryWorker>,
}

impl std::fmt::Debug for ChatHistoryShutdownFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ChatHistoryShutdownFailure")
            .field("retains_worker", &true)
            .finish()
    }
}

impl std::fmt::Display for ChatHistoryShutdownFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("chat history shutdown deadline exceeded")
    }
}

impl std::error::Error for ChatHistoryShutdownFailure {}

#[cfg(test)]
impl ChatHistoryShutdownFailure {
    fn into_worker_for_test(self) -> ChatHistoryWorker {
        let mut retained = std::mem::ManuallyDrop::new(self);
        unsafe { std::mem::ManuallyDrop::take(&mut retained._worker) }
    }
}

impl ChatHistory {
    pub fn spawn(path: PathBuf) -> Result<(Self, ChatHistoryWorker), HistoryError> {
        let (sender, receiver) = mpsc::sync_channel(HISTORY_QUEUE_CAPACITY);
        // Terminal writes have their own unbounded lane. A turn is never begun
        // unless this lane exists, so foreground history traffic cannot make
        // final persistence fail with `Busy`.
        let (terminal_sender, terminal_receiver) = mpsc::channel::<TerminalCommand>();
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let (completion_tx, completion) = mpsc::sync_channel(1);
        let join = thread::Builder::new()
            .name("loxa-chat-history".into())
            .spawn(move || {
                let _completion = HistoryCompletion(completion_tx);
                let mut repository = match ChatHistoryRepository::open(&path) {
                    Ok(repository) => repository,
                    Err(error) => {
                        let _ = ready_tx.send(Err(error));
                        return Err(error);
                    }
                };
                let recovered_at = unix_time_ms();
                if let Err(error) = repository.recover_interrupted(recovered_at) {
                    let _ = ready_tx.send(Err(error));
                    return Err(error);
                }
                let _ = ready_tx.send(Ok(()));
                #[cfg(test)]
                let mut fail_next_finalize = false;
                loop {
                    while let Ok(command) = terminal_receiver.try_recv() {
                        #[cfg(test)]
                        if std::mem::take(&mut fail_next_finalize) {
                            let _ = command.response.send(Err(HistoryError::Database));
                            continue;
                        }
                        let _ = command.response.send(repository.finalize_turn_with_metrics(
                            &command.turn,
                            command.state,
                            command.content,
                            command.code.as_deref(),
                            command.metrics,
                            command.at,
                        ));
                    }
                    let command = match receiver.recv_timeout(std::time::Duration::from_millis(10))
                    {
                        Ok(command) => command,
                        Err(mpsc::RecvTimeoutError::Timeout) => continue,
                        Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    };
                    match command {
                        Command::Create(at, tx) => {
                            let _ = tx.send(repository.create_chat(at));
                        }
                        Command::Get(id, tx) => {
                            let _ = tx.send(repository.get_chat(&id));
                        }
                        Command::List(limit, before, tx) => {
                            let _ = tx.send(repository.list_chats(limit, before.as_ref()));
                        }
                        Command::Rename(id, title, at, tx) => {
                            let _ = tx.send(repository.rename_chat(&id, title, at));
                        }
                        Command::Delete(id, tx) => {
                            let _ = tx.send(repository.delete_chat(&id));
                        }
                        Command::Clear(tx) => {
                            let _ = tx.send(repository.clear_all());
                        }
                        Command::Turns(id, limit, after, tx) => {
                            let _ = tx.send(repository.list_turns(&id, limit, after.as_ref()));
                        }
                        Command::Messages(id, tx) => {
                            let _ = tx.send(repository.message_summaries_for_turn(&id));
                        }
                        Command::GetTurn(id, tx) => {
                            let _ = tx.send(repository.get_turn(&id));
                        }
                        Command::MessagePage(id, segment, tx) => {
                            let _ = tx.send(repository.message_page(&id, segment));
                        }
                        Command::Begin(chat, content, provenance, at, tx) => {
                            let _ = tx.send(repository.begin_turn(&chat, content, provenance, at));
                        }
                        Command::Checkpoint(turn, content, at, tx) => {
                            let _ = tx.send(repository.checkpoint_assistant(&turn, content, at));
                        }
                        #[cfg(test)]
                        Command::FailNextFinalize(tx) => {
                            fail_next_finalize = true;
                            let _ = tx.send(());
                        }
                        Command::Stop => {
                            while let Ok(command) = terminal_receiver.try_recv() {
                                #[cfg(test)]
                                if std::mem::take(&mut fail_next_finalize) {
                                    let _ = command.response.send(Err(HistoryError::Database));
                                    continue;
                                }
                                let _ =
                                    command.response.send(repository.finalize_turn_with_metrics(
                                        &command.turn,
                                        command.state,
                                        command.content,
                                        command.code.as_deref(),
                                        command.metrics,
                                        command.at,
                                    ));
                            }
                            break;
                        }
                    }
                }
                Ok(())
            })
            .map_err(|_| HistoryError::Database)?;
        ready_rx.recv().map_err(|_| HistoryError::Database)??;
        Ok((
            Self {
                sender: sender.clone(),
                terminal_sender: terminal_sender.clone(),
            },
            ChatHistoryWorker {
                sender,
                terminal_sender,
                join: Some(join),
                completion,
            },
        ))
    }

    async fn call<T>(
        &self,
        make: impl FnOnce(oneshot::Sender<Result<T, HistoryError>>) -> Command,
    ) -> Result<T, ChatHistoryError> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .try_send(make(tx))
            .map_err(|error| match error {
                TrySendError::Full(_) => ChatHistoryError::Busy,
                TrySendError::Disconnected(_) => ChatHistoryError::Stopped,
            })?;
        rx.await
            .map_err(|_| ChatHistoryError::Stopped)?
            .map_err(ChatHistoryError::Repository)
    }

    pub async fn create_chat(&self, at: i64) -> Result<ChatSummary, ChatHistoryError> {
        self.call(|tx| Command::Create(at, tx)).await
    }
    pub async fn get_chat(&self, id: ChatId) -> Result<ChatSummary, ChatHistoryError> {
        self.call(|tx| Command::Get(id, tx)).await
    }
    pub async fn list_chats(
        &self,
        limit: usize,
        before: Option<ChatCursor>,
    ) -> Result<HistoryPage, ChatHistoryError> {
        self.call(|tx| Command::List(limit, before, tx)).await
    }
    pub async fn rename_chat(
        &self,
        id: ChatId,
        title: Title,
        at: i64,
    ) -> Result<ChatSummary, ChatHistoryError> {
        self.call(|tx| Command::Rename(id, title, at, tx)).await
    }
    pub async fn delete_chat(&self, id: ChatId) -> Result<(), ChatHistoryError> {
        self.call(|tx| Command::Delete(id, tx)).await
    }
    pub async fn clear_all(&self) -> Result<usize, ChatHistoryError> {
        self.call(Command::Clear).await
    }
    pub async fn list_turns(
        &self,
        id: ChatId,
        limit: usize,
        after: Option<TurnCursor>,
    ) -> Result<TurnPage, ChatHistoryError> {
        self.call(|tx| Command::Turns(id, limit, after, tx)).await
    }
    pub async fn message_summaries(
        &self,
        id: TurnId,
    ) -> Result<Vec<MessageSummary>, ChatHistoryError> {
        self.call(|tx| Command::Messages(id, tx)).await
    }
    pub async fn get_turn(&self, id: TurnId) -> Result<TurnRecord, ChatHistoryError> {
        self.call(|tx| Command::GetTurn(id, tx)).await
    }
    pub async fn message_page(
        &self,
        id: MessageId,
        segment: u32,
    ) -> Result<MessagePage, ChatHistoryError> {
        self.call(|tx| Command::MessagePage(id, segment, tx)).await
    }
    pub async fn begin_turn(
        &self,
        chat: ChatId,
        content: MessageContent,
        provenance: TurnProvenance,
        at: i64,
    ) -> Result<TurnRecord, ChatHistoryError> {
        self.call(|tx| Command::Begin(chat, content, provenance, at, tx))
            .await
    }
    pub async fn checkpoint(
        &self,
        turn: TurnId,
        content: MessageContent,
        at: i64,
    ) -> Result<(), ChatHistoryError> {
        self.call(|tx| Command::Checkpoint(turn, content, at, tx))
            .await
    }
    pub async fn finalize(
        &self,
        turn: TurnId,
        state: TurnState,
        content: MessageContent,
        code: Option<String>,
        metrics: TurnMetrics,
        at: i64,
    ) -> Result<(), ChatHistoryError> {
        let (response, receiver) = oneshot::channel();
        self.terminal_sender
            .send(TerminalCommand {
                turn,
                state,
                content,
                code,
                metrics,
                at,
                response,
            })
            .map_err(|_| ChatHistoryError::Stopped)?;
        receiver
            .await
            .map_err(|_| ChatHistoryError::Stopped)?
            .map_err(ChatHistoryError::Repository)
    }

    #[cfg(test)]
    pub(crate) async fn fail_next_finalize_for_test(&self) -> Result<(), ChatHistoryError> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .try_send(Command::FailNextFinalize(response))
            .map_err(|error| match error {
                TrySendError::Full(_) => ChatHistoryError::Busy,
                TrySendError::Disconnected(_) => ChatHistoryError::Stopped,
            })?;
        receiver.await.map_err(|_| ChatHistoryError::Stopped)
    }
}

impl ChatHistoryWorker {
    #[cfg(test)]
    pub(crate) fn poison_completion_for_test(&mut self) {
        let (never_complete, completion) = mpsc::sync_channel(1);
        self.completion = completion;
        std::mem::forget(never_complete);
    }

    pub(crate) fn request_shutdown(&mut self) -> bool {
        let requested = match self.sender.try_send(Command::Stop) {
            Ok(()) | Err(TrySendError::Disconnected(_)) => true,
            Err(TrySendError::Full(_)) => false,
        };
        if requested {
            let (replacement, _) = mpsc::channel();
            drop(std::mem::replace(&mut self.terminal_sender, replacement));
        }
        requested
    }

    pub(crate) fn shutdown_until(
        mut self,
        deadline: std::time::Instant,
    ) -> ChatHistoryShutdownResult {
        let mut stop = Command::Stop;
        loop {
            match self.sender.try_send(stop) {
                Ok(()) => break,
                Err(TrySendError::Full(returned)) => {
                    stop = returned;
                    let now = std::time::Instant::now();
                    if now >= deadline {
                        return ChatHistoryShutdownResult::Retained(ChatHistoryShutdownFailure {
                            _worker: std::mem::ManuallyDrop::new(self),
                        });
                    }
                    std::thread::sleep(std::time::Duration::from_millis(1).min(deadline - now));
                }
                Err(TrySendError::Disconnected(_)) => break,
            }
        }
        let (replacement, _) = mpsc::channel();
        drop(std::mem::replace(&mut self.terminal_sender, replacement));
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if self.completion.recv_timeout(remaining).is_err() {
            return ChatHistoryShutdownResult::Retained(ChatHistoryShutdownFailure {
                _worker: std::mem::ManuallyDrop::new(self),
            });
        }
        let join = self.join.take().expect("history worker join present");
        match join.join() {
            Ok(Ok(())) => ChatHistoryShutdownResult::Stopped,
            Ok(Err(error)) => ChatHistoryShutdownResult::Failed(error),
            Err(_) => ChatHistoryShutdownResult::Failed(HistoryError::Database),
        }
    }

    pub fn stop_and_join(mut self) -> Result<(), HistoryError> {
        let _ = self.sender.send(Command::Stop);
        let (replacement, _) = mpsc::channel();
        drop(std::mem::replace(&mut self.terminal_sender, replacement));
        self.join
            .take()
            .ok_or(HistoryError::Database)?
            .join()
            .map_err(|_| HistoryError::Database)??;
        Ok(())
    }
}

impl Drop for ChatHistoryWorker {
    fn drop(&mut self) {
        if let Some(join) = self.join.take() {
            std::mem::forget(join);
        }
    }
}

pub fn unix_time_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_history_path() -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::fs::canonicalize(std::env::temp_dir())
            .unwrap()
            .join(format!("loxa-node-history-{}-{nonce}", std::process::id()))
            .join("chat-history.sqlite3")
    }

    #[test]
    fn history_worker_shutdown_observes_completion_before_join() {
        let path = temp_history_path();
        let (_, worker) = ChatHistory::spawn(path.clone()).unwrap();
        assert!(matches!(
            worker.shutdown_until(std::time::Instant::now() + std::time::Duration::from_secs(1)),
            ChatHistoryShutdownResult::Stopped
        ));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn history_worker_deadline_retains_join_and_stop_capability() {
        let (sender, receiver) = mpsc::sync_channel(0);
        let (terminal_sender, _terminal_receiver) = mpsc::channel();
        let (completion_tx, completion) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let join = std::thread::spawn(move || {
            let _ = release_rx.recv();
            drop(completion_tx);
            drop(receiver);
            Ok(())
        });
        let worker = ChatHistoryWorker {
            sender,
            terminal_sender,
            join: Some(join),
            completion,
        };

        let failure = match worker.shutdown_until(std::time::Instant::now()) {
            ChatHistoryShutdownResult::Retained(failure) => failure,
            _ => panic!("expired shutdown must retain the history worker"),
        };
        let mut retained = failure.into_worker_for_test();
        assert!(retained.join.is_some());
        release_tx.send(()).unwrap();
        retained.join.take().unwrap().join().unwrap().unwrap();
    }

    #[test]
    fn history_worker_panic_is_reported_after_completion() {
        let (sender, receiver) = mpsc::sync_channel(1);
        let (terminal_sender, _terminal_receiver) = mpsc::channel();
        let (completion_tx, completion) = mpsc::channel();
        let join = std::thread::spawn(move || -> Result<(), HistoryError> {
            let _ = receiver.recv();
            completion_tx.send(()).unwrap();
            panic!("injected history panic")
        });
        let worker = ChatHistoryWorker {
            sender,
            terminal_sender,
            join: Some(join),
            completion,
        };

        assert!(matches!(
            worker.shutdown_until(std::time::Instant::now() + std::time::Duration::from_secs(1)),
            ChatHistoryShutdownResult::Failed(HistoryError::Database)
        ));
    }

    #[tokio::test]
    async fn worker_creates_and_reads_a_chat() {
        let path = temp_history_path();
        let (history, worker) = ChatHistory::spawn(path.clone()).unwrap();
        let chat = history.create_chat(1).await.unwrap();
        assert_eq!(history.get_chat(chat.id.clone()).await.unwrap(), chat);
        worker.stop_and_join().unwrap();
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn spawning_never_repairs_permissions_on_an_existing_unsafe_parent() {
        use std::os::unix::fs::PermissionsExt;

        let path = temp_history_path();
        let parent = path.parent().unwrap().to_owned();
        std::fs::create_dir_all(&parent).unwrap();
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o755)).unwrap();

        assert!(matches!(
            ChatHistory::spawn(path),
            Err(HistoryError::Security)
        ));
        assert_eq!(
            std::fs::metadata(&parent).unwrap().permissions().mode() & 0o777,
            0o755,
            "the node must reject, not chmod, an existing unsafe directory"
        );
        std::fs::remove_dir_all(parent).unwrap();
    }
}
