const MAX_VISIBLE_ITEMS: usize = 5;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct InstalledItem {
    id: String,
    display_name: String,
    total_bytes: u64,
}

impl InstalledItem {
    pub(crate) fn new(id: String, display_name: String, total_bytes: u64) -> Self {
        Self {
            id,
            display_name,
            total_bytes,
        }
    }

    pub(crate) fn id(&self) -> &str {
        &self.id
    }

    pub(crate) fn display_name(&self) -> &str {
        &self.display_name
    }

    pub(crate) fn total_bytes(&self) -> u64 {
        self.total_bytes
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InstalledInventoryError {
    RefreshFailed,
}

impl InstalledInventoryError {
    pub(crate) fn message(self) -> &'static str {
        match self {
            Self::RefreshFailed => "Could not refresh installed models",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InstalledFeedback {
    ChatCommandCopied,
    CopyFailed,
    RevealFailed,
}

impl InstalledFeedback {
    fn message(self) -> &'static str {
        match self {
            Self::ChatCommandCopied => "Chat command copied",
            Self::CopyFailed => "Could not copy the chat command",
            Self::RevealFailed => "Could not reveal this model",
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct InstalledState {
    items: Vec<InstalledItem>,
    selected_model_id: Option<String>,
    pinned_model_id: Option<String>,
    error: Option<InstalledInventoryError>,
    feedback: Option<InstalledFeedback>,
}

impl InstalledState {
    pub(crate) fn replace(
        &mut self,
        mut items: Vec<InstalledItem>,
        pinned_model_id: Option<String>,
    ) {
        items.sort_by(|left, right| left.id().cmp(right.id()));
        let previous_selection = self.selected_model_id.clone();
        self.selected_model_id = self
            .selected_model_id
            .take()
            .filter(|selected| items.iter().any(|item| item.id() == selected));
        if self.selected_model_id != previous_selection {
            self.feedback = None;
        }
        self.pinned_model_id =
            pinned_model_id.filter(|pinned| items.iter().any(|item| item.id() == pinned));
        self.items = items;
        self.error = None;
    }

    pub(crate) fn fail(&mut self, error: InstalledInventoryError) {
        self.error = Some(error);
    }

    #[cfg(test)]
    pub(crate) fn items(&self) -> &[InstalledItem] {
        &self.items
    }

    pub(crate) fn visible_items(&self) -> Vec<&InstalledItem> {
        self.visible_items_for(None)
    }

    pub(crate) fn visible_items_for(&self, active_model_id: Option<&str>) -> Vec<&InstalledItem> {
        let active = active_model_id.and_then(|id| self.items.iter().find(|item| item.id() == id));
        let pinned = self
            .pinned_model_id
            .as_deref()
            .and_then(|id| self.items.iter().find(|item| item.id() == id))
            .filter(|item| Some(item.id()) != active_model_id);
        active
            .into_iter()
            .chain(pinned)
            .chain(
                self.items
                    .iter()
                    .filter(|item| Some(item.id()) != active_model_id)
                    .filter(|item| Some(item.id()) != self.pinned_model_id.as_deref()),
            )
            .take(MAX_VISIBLE_ITEMS)
            .collect()
    }

    pub(crate) fn remaining_count(&self) -> usize {
        self.items.len().saturating_sub(MAX_VISIBLE_ITEMS)
    }

    pub(crate) fn select(&mut self, model_id: &str) -> bool {
        if !self.items.iter().any(|item| item.id() == model_id) {
            return false;
        }
        if self.selected_model_id.as_deref() != Some(model_id) {
            self.feedback = None;
            self.selected_model_id = Some(model_id.into());
        }
        true
    }

    pub(crate) fn selected(&self) -> Option<&InstalledItem> {
        let selected = self.selected_model_id.as_deref()?;
        self.items.iter().find(|item| item.id() == selected)
    }

    pub(crate) fn error_message(&self) -> Option<&'static str> {
        self.error.map(InstalledInventoryError::message)
    }

    pub(crate) fn apply_feedback(
        &mut self,
        model_id: &str,
        feedback: Option<InstalledFeedback>,
    ) -> bool {
        if self.selected().map(InstalledItem::id) != Some(model_id) {
            return false;
        }
        self.feedback = feedback;
        true
    }

    pub(crate) fn reset_feedback(&mut self) {
        self.feedback = None;
    }

    pub(crate) fn feedback_message(&self) -> Option<&'static str> {
        self.feedback.map(InstalledFeedback::message)
    }
}

#[cfg(test)]
mod tests {
    use super::{InstalledInventoryError, InstalledItem, InstalledState};

    fn item(id: &str) -> InstalledItem {
        InstalledItem::new(id.into(), format!("{id}.gguf"), 42)
    }

    #[test]
    fn replacement_is_sorted_and_clears_a_selection_missing_from_the_new_inventory() {
        let mut state = InstalledState::default();

        state.replace(vec![item("beta"), item("alpha")], None);
        assert_eq!(
            state
                .items()
                .iter()
                .map(InstalledItem::id)
                .collect::<Vec<_>>(),
            ["alpha", "beta"]
        );
        assert!(state.select("beta"));
        assert_eq!(state.selected().map(InstalledItem::id), Some("beta"));

        state.replace(vec![item("alpha")], None);
        assert_eq!(state.selected(), None);
    }

    #[test]
    fn refresh_failure_retains_the_last_good_inventory_and_exact_selection() {
        let mut state = InstalledState::default();
        state.replace(vec![item("alpha"), item("beta")], None);
        assert!(state.select("beta"));

        state.fail(InstalledInventoryError::RefreshFailed);

        assert_eq!(
            state
                .items()
                .iter()
                .map(InstalledItem::id)
                .collect::<Vec<_>>(),
            ["alpha", "beta"]
        );
        assert_eq!(state.selected().map(InstalledItem::id), Some("beta"));
        assert_eq!(
            state.error_message(),
            Some("Could not refresh installed models")
        );
    }

    #[test]
    fn visible_inventory_is_bounded_and_pins_the_exact_completed_model_first() {
        let mut state = InstalledState::default();
        state.replace(
            [
                "delta", "alpha", "foxtrot", "charlie", "golf", "bravo", "echo",
            ]
            .into_iter()
            .map(item)
            .collect(),
            Some("foxtrot".into()),
        );

        assert_eq!(
            state
                .visible_items()
                .into_iter()
                .map(InstalledItem::id)
                .collect::<Vec<_>>(),
            ["foxtrot", "alpha", "bravo", "charlie", "delta"]
        );
        assert_eq!(state.remaining_count(), 2);
        assert_eq!(state.selected(), None);
        assert_eq!(state.visible_items()[0].display_name(), "foxtrot.gguf");
        assert_eq!(state.visible_items()[0].total_bytes(), 42);
    }

    #[test]
    fn active_model_beyond_the_default_cap_is_first_and_stays_addressable() {
        let mut state = InstalledState::default();
        state.replace(
            ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot"]
                .into_iter()
                .map(item)
                .collect(),
            Some("bravo".into()),
        );

        let visible = state.visible_items_for(Some("foxtrot"));
        assert_eq!(
            visible.iter().map(|item| item.id()).collect::<Vec<_>>(),
            ["foxtrot", "bravo", "alpha", "charlie", "delta"]
        );
        assert_eq!(visible.len(), 5);
    }
}
