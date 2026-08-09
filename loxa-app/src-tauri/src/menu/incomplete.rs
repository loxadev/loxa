const MAX_VISIBLE_ITEMS: usize = 3;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct IncompleteItem {
    id: String,
    completed_bytes: u64,
    total_bytes: u64,
}

impl IncompleteItem {
    pub(crate) fn new(id: String, completed_bytes: u64, total_bytes: u64) -> Self {
        Self {
            id,
            completed_bytes,
            total_bytes,
        }
    }

    pub(crate) fn id(&self) -> &str {
        &self.id
    }

    pub(crate) fn completed_bytes(&self) -> u64 {
        self.completed_bytes
    }

    pub(crate) fn total_bytes(&self) -> u64 {
        self.total_bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IncompleteInventoryError {
    RefreshFailed,
}

impl IncompleteInventoryError {
    pub(crate) fn message(self) -> &'static str {
        "Could not refresh incomplete downloads"
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DiscardFailure {
    Unavailable,
}

impl DiscardFailure {
    fn message(self) -> &'static str {
        match self {
            Self::Unavailable => "Could not finish discarding. Refresh and try again.",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DiscardState {
    Preparing(String),
    Confirming(String),
    Discarding(String),
}

impl DiscardState {
    fn model_id(&self) -> &str {
        match self {
            Self::Preparing(model_id) | Self::Confirming(model_id) | Self::Discarding(model_id) => {
                model_id
            }
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct IncompleteState {
    items: Vec<IncompleteItem>,
    discard: Option<DiscardState>,
    inventory_error: Option<IncompleteInventoryError>,
    discard_failure: Option<DiscardFailure>,
}

impl IncompleteState {
    pub(crate) fn has_content(&self) -> bool {
        !self.items.is_empty() || self.inventory_error.is_some() || self.discard_failure.is_some()
    }

    pub(crate) fn discard_is_active(&self) -> bool {
        self.discard.is_some()
    }

    pub(crate) fn replace(&mut self, mut items: Vec<IncompleteItem>) {
        items.sort_by(|left, right| left.id.cmp(&right.id));
        if self
            .discard
            .as_ref()
            .is_some_and(|discard| !items.iter().any(|item| item.id() == discard.model_id()))
        {
            self.discard = None;
            self.discard_failure = None;
        }
        self.items = items;
        self.inventory_error = None;
    }

    pub(crate) fn fail(&mut self, error: IncompleteInventoryError) {
        self.inventory_error = Some(error);
    }

    pub(crate) fn visible_items(&self) -> Vec<&IncompleteItem> {
        self.items.iter().take(MAX_VISIBLE_ITEMS).collect()
    }

    pub(crate) fn remaining_count(&self) -> usize {
        self.items.len().saturating_sub(MAX_VISIBLE_ITEMS)
    }

    pub(crate) fn prepare_discard(&mut self, visible_index: usize) -> Option<String> {
        if self.discard.is_some() {
            return None;
        }
        let model_id = self.visible_items().get(visible_index)?.id().to_owned();
        self.discard_failure = None;
        self.discard = Some(DiscardState::Preparing(model_id.clone()));
        Some(model_id)
    }

    pub(crate) fn prepared(&mut self, model_id: &str, result: Result<(), DiscardFailure>) -> bool {
        if !matches!(
            self.discard.as_ref(),
            Some(DiscardState::Preparing(current)) if current == model_id
        ) {
            return false;
        }
        match result {
            Ok(()) => self.discard = Some(DiscardState::Confirming(model_id.into())),
            Err(failure) => {
                self.discard = None;
                self.discard_failure = Some(failure);
            }
        }
        true
    }

    pub(crate) fn keep_partial(&mut self) -> Option<String> {
        let model_id = match self.discard.as_ref()? {
            DiscardState::Confirming(model_id) => model_id.clone(),
            DiscardState::Preparing(_) | DiscardState::Discarding(_) => return None,
        };
        self.discard = None;
        Some(model_id)
    }

    pub(crate) fn cancel_prepared(&mut self) -> Option<String> {
        let discard = self.discard.take()?;
        match discard {
            DiscardState::Preparing(model_id) | DiscardState::Confirming(model_id) => {
                Some(model_id)
            }
            DiscardState::Discarding(model_id) => {
                self.discard = Some(DiscardState::Discarding(model_id));
                None
            }
        }
    }

    pub(crate) fn confirm_discard(&mut self) -> Option<String> {
        let model_id = match self.discard.as_ref()? {
            DiscardState::Confirming(model_id) => model_id.clone(),
            DiscardState::Preparing(_) | DiscardState::Discarding(_) => return None,
        };
        self.discard = Some(DiscardState::Discarding(model_id.clone()));
        Some(model_id)
    }

    pub(crate) fn completed(&mut self, model_id: &str, result: Result<(), DiscardFailure>) -> bool {
        if !matches!(
            self.discard.as_ref(),
            Some(DiscardState::Discarding(current)) if current == model_id
        ) {
            return false;
        }
        self.discard = None;
        match result {
            Ok(()) => {
                self.items.retain(|item| item.id() != model_id);
                self.discard_failure = None;
            }
            Err(failure) => self.discard_failure = Some(failure),
        }
        true
    }

    pub(crate) fn confirmation_model_id(&self) -> Option<&str> {
        match self.discard.as_ref() {
            Some(DiscardState::Confirming(model_id)) => Some(model_id),
            _ => None,
        }
    }

    pub(crate) fn is_preparing(&self, model_id: &str) -> bool {
        matches!(self.discard.as_ref(), Some(DiscardState::Preparing(current)) if current == model_id)
    }

    pub(crate) fn is_discarding(&self, model_id: &str) -> bool {
        matches!(self.discard.as_ref(), Some(DiscardState::Discarding(current)) if current == model_id)
    }

    pub(crate) fn inventory_error_message(&self) -> Option<&'static str> {
        self.inventory_error.map(IncompleteInventoryError::message)
    }

    pub(crate) fn feedback_message(&self) -> Option<&'static str> {
        self.discard_failure.map(DiscardFailure::message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(id: &str, completed: u64, total: u64) -> IncompleteItem {
        IncompleteItem::new(id.into(), completed, total)
    }

    #[test]
    fn inventory_is_sorted_bounded_and_preserves_exact_progress() {
        let mut state = IncompleteState::default();
        state.replace(vec![
            item("delta", 4, 10),
            item("alpha", 1, 10),
            item("charlie", 3, 10),
            item("bravo", 2, 10),
        ]);

        assert_eq!(
            state
                .visible_items()
                .into_iter()
                .map(IncompleteItem::id)
                .collect::<Vec<_>>(),
            ["alpha", "bravo", "charlie"]
        );
        assert_eq!(state.remaining_count(), 1);
        assert_eq!(state.visible_items()[1].completed_bytes(), 2);
        assert_eq!(state.visible_items()[1].total_bytes(), 10);
        state.fail(IncompleteInventoryError::RefreshFailed);
        assert_eq!(
            state.inventory_error_message(),
            Some("Could not refresh incomplete downloads")
        );
    }

    #[test]
    fn discard_requires_prepare_and_consumes_only_the_exact_confirmation() {
        let mut state = IncompleteState::default();
        state.replace(vec![item("alpha", 2, 10), item("beta", 3, 10)]);

        assert_eq!(state.confirm_discard(), None);
        assert_eq!(state.prepare_discard(1), Some("beta".into()));
        assert_eq!(state.confirm_discard(), None);
        assert!(!state.prepared("alpha", Ok(())));
        assert!(state.prepared("beta", Ok(())));
        assert_eq!(state.confirmation_model_id(), Some("beta"));
        assert_eq!(state.confirm_discard(), Some("beta".into()));
        assert_eq!(state.confirm_discard(), None);
        assert!(!state.completed("alpha", Ok(())));
        assert!(state.completed("beta", Err(DiscardFailure::Unavailable)));
        assert_eq!(
            state.feedback_message(),
            Some("Could not finish discarding. Refresh and try again.")
        );
    }

    #[test]
    fn keep_cancels_without_confirming_and_refresh_removes_stale_prompt() {
        let mut state = IncompleteState::default();
        state.replace(vec![item("alpha", 2, 10)]);
        assert_eq!(state.prepare_discard(0), Some("alpha".into()));
        assert!(state.prepared("alpha", Ok(())));

        assert_eq!(state.keep_partial(), Some("alpha".into()));
        assert_eq!(state.confirm_discard(), None);

        assert_eq!(state.prepare_discard(0), Some("alpha".into()));
        assert!(state.prepared("alpha", Ok(())));
        state.replace(Vec::new());
        assert_eq!(state.confirmation_model_id(), None);
    }

    #[test]
    fn successful_discard_removes_only_the_exact_row_before_a_failed_refresh() {
        let mut state = IncompleteState::default();
        state.replace(vec![item("alpha", 2, 10), item("beta", 3, 10)]);
        assert_eq!(state.prepare_discard(0).as_deref(), Some("alpha"));
        assert!(state.discard_is_active());
        assert!(state.prepared("alpha", Ok(())));
        assert_eq!(state.confirm_discard().as_deref(), Some("alpha"));

        assert!(state.completed("alpha", Ok(())));
        state.fail(IncompleteInventoryError::RefreshFailed);

        assert_eq!(
            state
                .visible_items()
                .into_iter()
                .map(IncompleteItem::id)
                .collect::<Vec<_>>(),
            ["beta"]
        );
        assert!(!state.discard_is_active());
        assert_eq!(
            state.inventory_error_message(),
            Some("Could not refresh incomplete downloads")
        );
    }
}
