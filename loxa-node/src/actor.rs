use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Mutation {
    Download { model_id: String },
    Load { model_id: String },
    Unload,
}

#[derive(Clone, Debug)]
pub struct MutationCancellation(Arc<AtomicBool>);

impl MutationCancellation {
    fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }

    fn cancel(&self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubmitError {
    Conflict,
    Stopping,
}

pub trait MutationExecutor: Send + 'static {
    fn execute(
        &mut self,
        operation_id: &str,
        mutation: &Mutation,
        cancellation: &MutationCancellation,
    );
}

struct PendingMutation {
    operation_id: String,
    mutation: Mutation,
    cancellation: MutationCancellation,
}

#[derive(Default)]
struct ActorState {
    pending: VecDeque<PendingMutation>,
    tracked: HashMap<Mutation, MutationCancellation>,
    stopping: bool,
}

struct Shared {
    state: Mutex<ActorState>,
    changed: Condvar,
}

#[derive(Clone)]
pub struct NodeActorHandle(Arc<Shared>);

impl NodeActorHandle {
    pub fn submit(
        &self,
        operation_id: impl Into<String>,
        mutation: Mutation,
    ) -> Result<(), SubmitError> {
        let mut state = self.0.state.lock().expect("node actor lock poisoned");
        if state.stopping {
            return Err(SubmitError::Stopping);
        }
        if state.tracked.contains_key(&mutation) {
            return Err(SubmitError::Conflict);
        }
        let cancellation = MutationCancellation::new();
        state.tracked.insert(mutation.clone(), cancellation.clone());
        state.pending.push_back(PendingMutation {
            operation_id: operation_id.into(),
            mutation,
            cancellation,
        });
        self.0.changed.notify_one();
        Ok(())
    }

    pub fn stop(&self) {
        let mut state = self.0.state.lock().expect("node actor lock poisoned");
        if state.stopping {
            return;
        }
        state.stopping = true;
        for cancellation in state.tracked.values() {
            cancellation.cancel();
        }
        state.pending.clear();
        self.0.changed.notify_one();
    }
}

pub struct NodeActor;

impl NodeActor {
    pub fn spawn<E: MutationExecutor>(mut executor: E) -> (NodeActorHandle, JoinHandle<()>) {
        let shared = Arc::new(Shared {
            state: Mutex::new(ActorState::default()),
            changed: Condvar::new(),
        });
        let handle = NodeActorHandle(Arc::clone(&shared));
        let worker = thread::spawn(move || loop {
            let command = {
                let mut state = shared.state.lock().expect("node actor lock poisoned");
                while state.pending.is_empty() && !state.stopping {
                    state = shared
                        .changed
                        .wait(state)
                        .expect("node actor lock poisoned");
                }
                if state.stopping {
                    return;
                }
                state.pending.pop_front().expect("pending command exists")
            };
            executor.execute(
                &command.operation_id,
                &command.mutation,
                &command.cancellation,
            );
            let mut state = shared.state.lock().expect("node actor lock poisoned");
            state.tracked.remove(&command.mutation);
        });
        (handle, worker)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::Duration;

    struct BlockingExecutor {
        started: mpsc::Sender<(String, MutationCancellation)>,
        release: mpsc::Receiver<()>,
        running: Arc<AtomicBool>,
        overlap: Arc<AtomicBool>,
    }

    impl MutationExecutor for BlockingExecutor {
        fn execute(&mut self, id: &str, _: &Mutation, cancellation: &MutationCancellation) {
            if self.running.swap(true, Ordering::SeqCst) {
                self.overlap.store(true, Ordering::SeqCst);
            }
            self.started
                .send((id.to_owned(), cancellation.clone()))
                .unwrap();
            while self.release.recv_timeout(Duration::from_millis(5)).is_err()
                && !cancellation.is_cancelled()
            {}
            self.running.store(false, Ordering::SeqCst);
        }
    }

    fn load(id: &str) -> Mutation {
        Mutation::Load {
            model_id: id.into(),
        }
    }

    #[test]
    fn actor_executes_fifo_without_overlap_and_releases_duplicate_after_completion() {
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let overlap = Arc::new(AtomicBool::new(false));
        let (handle, worker) = NodeActor::spawn(BlockingExecutor {
            started: started_tx,
            release: release_rx,
            running: Arc::new(AtomicBool::new(false)),
            overlap: Arc::clone(&overlap),
        });
        handle.submit("one", load("a")).unwrap();
        handle.submit("two", load("b")).unwrap();
        assert_eq!(handle.submit("dup", load("a")), Err(SubmitError::Conflict));
        assert_eq!(started_rx.recv().unwrap().0, "one");
        release_tx.send(()).unwrap();
        assert_eq!(started_rx.recv().unwrap().0, "two");
        release_tx.send(()).unwrap();
        handle.stop();
        worker.join().unwrap();
        assert!(!overlap.load(Ordering::SeqCst));
    }

    #[test]
    fn stop_cancels_active_drops_pending_rejects_new_and_joins() {
        let (started_tx, started_rx) = mpsc::channel();
        let (_release_tx, release_rx) = mpsc::channel();
        let (handle, worker) = NodeActor::spawn(BlockingExecutor {
            started: started_tx,
            release: release_rx,
            running: Arc::new(AtomicBool::new(false)),
            overlap: Arc::new(AtomicBool::new(false)),
        });
        handle.submit("active", load("a")).unwrap();
        handle.submit("pending", load("b")).unwrap();
        let (_, cancellation) = started_rx.recv().unwrap();
        handle.stop();
        assert_eq!(handle.submit("late", load("c")), Err(SubmitError::Stopping));
        worker.join().unwrap();
        assert!(cancellation.is_cancelled());
        assert!(
            started_rx.try_recv().is_err(),
            "pending mutation must be dropped"
        );
    }
}
