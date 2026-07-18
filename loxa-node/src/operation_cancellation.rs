use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

const CANCELLATION_OPEN: u8 = 0;
const CANCELLATION_REQUESTED: u8 = 1;
const TERMINAL_CLAIMED: u8 = 2;

#[derive(Clone, Debug)]
pub(crate) struct OperationCancellation(Arc<AtomicU8>);

impl OperationCancellation {
    pub(crate) fn new() -> Self {
        Self(Arc::new(AtomicU8::new(CANCELLATION_OPEN)))
    }

    pub(crate) fn is_cancel_requested(&self) -> bool {
        self.0.load(Ordering::SeqCst) == CANCELLATION_REQUESTED
    }

    pub(crate) fn request_cancel(&self) -> bool {
        self.0
            .compare_exchange(
                CANCELLATION_OPEN,
                CANCELLATION_REQUESTED,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok()
    }

    pub(crate) fn claim_terminal(&self) -> bool {
        self.0
            .compare_exchange(
                CANCELLATION_OPEN,
                TERMINAL_CLAIMED,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok()
    }
}
