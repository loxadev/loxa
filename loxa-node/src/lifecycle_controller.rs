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
        LifecycleVerificationCompletion::reserve(&self.verification, self)
    }

    pub(super) fn notify_verification_ready(&self) -> bool {
        self.verification.notify_ready()
    }

    pub(super) fn rollback_verification(&self, completion: &LifecycleVerificationCompletion) {
        completion.rollback_from(&self.verification);
    }
}
