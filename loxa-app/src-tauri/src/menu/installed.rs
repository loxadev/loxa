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

    // Task 3 native rows consume this surface.
    #[allow(dead_code)]
    pub(crate) fn display_name(&self) -> &str {
        &self.display_name
    }

    // Task 3 native rows consume this surface.
    #[allow(dead_code)]
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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct InstalledState {
    items: Vec<InstalledItem>,
    selected_model_id: Option<String>,
    pinned_model_id: Option<String>,
    error: Option<InstalledInventoryError>,
}

impl InstalledState {
    pub(crate) fn replace(
        &mut self,
        mut items: Vec<InstalledItem>,
        pinned_model_id: Option<String>,
    ) {
        items.sort_by(|left, right| left.id().cmp(right.id()));
        self.selected_model_id = self
            .selected_model_id
            .take()
            .filter(|selected| items.iter().any(|item| item.id() == selected));
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

    // Task 3 native rows consume this surface.
    #[allow(dead_code)]
    pub(crate) fn visible_items(&self) -> Vec<&InstalledItem> {
        let pinned = self
            .pinned_model_id
            .as_deref()
            .and_then(|id| self.items.iter().find(|item| item.id() == id));
        pinned
            .into_iter()
            .chain(
                self.items
                    .iter()
                    .filter(|item| Some(item.id()) != self.pinned_model_id.as_deref()),
            )
            .take(MAX_VISIBLE_ITEMS)
            .collect()
    }

    // Task 3 native rows consume this surface.
    #[allow(dead_code)]
    pub(crate) fn remaining_count(&self) -> usize {
        self.items.len().saturating_sub(MAX_VISIBLE_ITEMS)
    }

    // Task 3 native rows consume this surface.
    #[allow(dead_code)]
    pub(crate) fn select(&mut self, model_id: &str) -> bool {
        if !self.items.iter().any(|item| item.id() == model_id) {
            return false;
        }
        self.selected_model_id = Some(model_id.into());
        true
    }

    // Task 3 native rows consume this surface.
    #[allow(dead_code)]
    pub(crate) fn selected(&self) -> Option<&InstalledItem> {
        let selected = self.selected_model_id.as_deref()?;
        self.items.iter().find(|item| item.id() == selected)
    }

    // Task 3 native rows consume this surface.
    #[allow(dead_code)]
    pub(crate) fn error_message(&self) -> Option<&'static str> {
        self.error.map(InstalledInventoryError::message)
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
}
