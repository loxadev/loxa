use crate::verification_scheduler::{
    CompletionDestination, LifecycleVerificationCompletion, LifecycleVerificationOutcome,
};
use std::sync::Arc;

pub(crate) struct LifecycleMailboxInner {
    verification: CompletionDestination<LifecycleVerificationOutcome>,
}

impl LifecycleMailboxInner {
    pub(crate) fn new(verification_capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            verification: CompletionDestination::new(verification_capacity),
        })
    }

    pub(crate) fn reserve_verification(
        self: &Arc<Self>,
    ) -> Option<LifecycleVerificationCompletion> {
        let cell = self.verification.reserve()?;
        Some(LifecycleVerificationCompletion::new(
            cell,
            Arc::downgrade(self),
        ))
    }

    pub(super) fn notify_verification_ready(&self) -> bool {
        self.verification.notify_ready()
    }
}
