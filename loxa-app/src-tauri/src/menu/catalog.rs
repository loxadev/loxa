#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RepositoryItem {
    repo: String,
    detail: Option<String>,
}

impl RepositoryItem {
    pub(crate) fn new(repo: String, detail: Option<String>) -> Self {
        Self { repo, detail }
    }

    pub(crate) fn repo(&self) -> &str {
        &self.repo
    }

    pub(crate) fn detail(&self) -> Option<&str> {
        self.detail.as_deref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CandidateItem {
    path: String,
    size: Option<u64>,
    installed_model_id: Option<String>,
}

impl CandidateItem {
    pub(crate) fn new(path: String, size: Option<u64>) -> Self {
        Self {
            path,
            size,
            installed_model_id: None,
        }
    }

    pub(crate) fn with_installed_model_id(mut self, model_id: String) -> Self {
        self.installed_model_id = Some(model_id);
        self
    }

    pub(crate) fn path(&self) -> &str {
        &self.path
    }

    pub(crate) fn size(&self) -> Option<u64> {
        self.size
    }

    pub(crate) fn installed_model_id(&self) -> Option<&str> {
        self.installed_model_id.as_deref()
    }

    pub(crate) fn is_installed(&self) -> bool {
        self.installed_model_id().is_some()
    }

    fn transfer_action_label(&self) -> String {
        if self.is_installed() {
            return "Check installed".into();
        }
        self.size
            .map(|bytes| format!("Download {}", format_size(bytes)))
            .unwrap_or_else(|| "Download".into())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CatalogCommand {
    Search {
        generation: u64,
        query: String,
    },
    Inspect {
        generation: u64,
        repo: String,
    },
    Transfer {
        generation: u64,
        repo: String,
        revision: String,
        path: String,
    },
}

impl CatalogCommand {
    pub(crate) fn generation(&self) -> u64 {
        match self {
            Self::Search { generation, .. }
            | Self::Inspect { generation, .. }
            | Self::Transfer { generation, .. } => *generation,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransferStage {
    Transferring,
    Verifying,
    Publishing,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CatalogTransferDisposition {
    Installed,
    AlreadyInstalled,
    Paused,
    Interrupted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CatalogEvent {
    Repositories {
        generation: u64,
        repositories: Vec<RepositoryItem>,
    },
    Candidates {
        generation: u64,
        repo: String,
        revision: String,
        candidates: Vec<CandidateItem>,
    },
    Progress {
        generation: u64,
        stage: TransferStage,
        transferred_bytes: u64,
        total_bytes: u64,
    },
    Completed {
        generation: u64,
        disposition: CatalogTransferDisposition,
    },
    Failed {
        generation: u64,
        message: String,
    },
}

const MAX_VISIBLE_RESULTS: usize = 5;

#[derive(Clone, Debug, Eq, PartialEq)]
enum CatalogStatus {
    Idle,
    InvalidQuery,
    Searching,
    Repositories,
    Inspecting(String),
    Candidates,
    Selected(String),
    Resolving(String),
    Progress {
        stage: TransferStage,
        transferred_bytes: u64,
        total_bytes: u64,
    },
    PauseRequested,
    Completed(CatalogTransferDisposition),
    Error(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CatalogMode {
    Idle,
    Hint,
    Searching,
    Repositories,
    Inspecting,
    Candidates,
    Selected,
    Transferring,
    Completed,
    Error,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CatalogState {
    generation: u64,
    query: String,
    repositories: Vec<RepositoryItem>,
    repo: Option<String>,
    revision: Option<String>,
    candidates: Vec<CandidateItem>,
    selected: Option<usize>,
    status: CatalogStatus,
}

impl Default for CatalogState {
    fn default() -> Self {
        Self {
            generation: 0,
            query: String::new(),
            repositories: Vec::new(),
            repo: None,
            revision: None,
            candidates: Vec::new(),
            selected: None,
            status: CatalogStatus::Idle,
        }
    }
}

impl CatalogState {
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn query(&self) -> &str {
        &self.query
    }

    pub(crate) fn repositories(&self) -> &[RepositoryItem] {
        &self.repositories
    }

    pub(crate) fn candidates(&self) -> &[CandidateItem] {
        &self.candidates
    }

    pub(crate) fn selected_candidate(&self) -> Option<&CandidateItem> {
        self.selected.and_then(|index| self.candidates.get(index))
    }

    pub(crate) fn mode(&self) -> CatalogMode {
        match self.status {
            CatalogStatus::Idle => CatalogMode::Idle,
            CatalogStatus::InvalidQuery => CatalogMode::Hint,
            CatalogStatus::Searching => CatalogMode::Searching,
            CatalogStatus::Repositories => CatalogMode::Repositories,
            CatalogStatus::Inspecting(_) => CatalogMode::Inspecting,
            CatalogStatus::Candidates => CatalogMode::Candidates,
            CatalogStatus::Selected(_) => CatalogMode::Selected,
            CatalogStatus::Resolving(_)
            | CatalogStatus::Progress { .. }
            | CatalogStatus::PauseRequested => CatalogMode::Transferring,
            CatalogStatus::Completed(_) => CatalogMode::Completed,
            CatalogStatus::Error(_) => CatalogMode::Error,
        }
    }

    pub(crate) fn owns_main_region(&self) -> bool {
        matches!(
            self.mode(),
            CatalogMode::Searching
                | CatalogMode::Repositories
                | CatalogMode::Inspecting
                | CatalogMode::Candidates
                | CatalogMode::Selected
                | CatalogMode::Transferring
        )
    }

    pub(crate) fn browser_heading(&self) -> Option<&'static str> {
        match self.mode() {
            CatalogMode::Searching | CatalogMode::Repositories => Some("Models"),
            CatalogMode::Inspecting | CatalogMode::Candidates | CatalogMode::Selected => {
                Some("Choose a GGUF file")
            }
            CatalogMode::Idle
            | CatalogMode::Hint
            | CatalogMode::Transferring
            | CatalogMode::Completed
            | CatalogMode::Error => None,
        }
    }

    pub(crate) fn browser_status(&self) -> Option<String> {
        match self.mode() {
            CatalogMode::Idle | CatalogMode::Selected => None,
            CatalogMode::Hint
            | CatalogMode::Searching
            | CatalogMode::Transferring
            | CatalogMode::Completed
            | CatalogMode::Error => Some(self.status_label()),
            CatalogMode::Repositories if self.repositories.is_empty() => Some(self.status_label()),
            CatalogMode::Repositories => None,
            CatalogMode::Inspecting => Some("Loading files…".into()),
            CatalogMode::Candidates if self.candidates.is_empty() => Some(self.status_label()),
            CatalogMode::Candidates => None,
        }
    }

    pub(crate) fn shows_repository_rows(&self) -> bool {
        self.mode() == CatalogMode::Repositories
    }

    pub(crate) fn shows_candidate_rows(&self) -> bool {
        matches!(self.mode(), CatalogMode::Candidates | CatalogMode::Selected)
    }

    pub(crate) fn status_label(&self) -> String {
        match &self.status {
            CatalogStatus::Idle => "Search Hugging Face".into(),
            CatalogStatus::InvalidQuery => "Type at least 2 characters".into(),
            CatalogStatus::Searching => "Searching Hugging Face…".into(),
            CatalogStatus::Repositories => {
                if self.repositories.is_empty() {
                    "No repositories found".into()
                } else {
                    "Choose a repository".into()
                }
            }
            CatalogStatus::Inspecting(repo) => format!("Inspecting {repo}…"),
            CatalogStatus::Candidates => {
                if self.candidates.is_empty() {
                    "No eligible single-file GGUF found".into()
                } else {
                    "Choose a GGUF file".into()
                }
            }
            CatalogStatus::Selected(_) => "Ready to download".into(),
            CatalogStatus::Resolving(path) => format!("Preparing {path}…"),
            CatalogStatus::Progress {
                stage,
                transferred_bytes,
                total_bytes,
            } => format!(
                "{} {} of {} bytes",
                stage.label(),
                transferred_bytes,
                total_bytes
            ),
            CatalogStatus::PauseRequested => "Pausing transfer…".into(),
            CatalogStatus::Completed(disposition) => disposition.label().into(),
            CatalogStatus::Error(message) => message.clone(),
        }
    }

    pub(crate) fn progress_fraction(&self) -> Option<f64> {
        let CatalogStatus::Progress {
            transferred_bytes,
            total_bytes,
            ..
        } = self.status
        else {
            return None;
        };
        if total_bytes == 0 {
            return Some(0.0);
        }
        Some((transferred_bytes.min(total_bytes) as f64) / (total_bytes as f64))
    }

    pub(crate) fn can_transfer(&self) -> bool {
        matches!(self.status, CatalogStatus::Selected(_)) && self.selected_candidate().is_some()
    }

    pub(crate) fn transfer_action_label(&self) -> Option<String> {
        if !self.can_transfer() {
            return None;
        }
        Some(self.selected_candidate()?.transfer_action_label())
    }

    pub(crate) fn can_pause(&self) -> bool {
        matches!(
            self.status,
            CatalogStatus::Resolving(_) | CatalogStatus::Progress { .. }
        )
    }

    pub(crate) fn submit_search(&mut self, query: &str) -> Option<CatalogCommand> {
        if matches!(
            self.status,
            CatalogStatus::Resolving(_)
                | CatalogStatus::Progress { .. }
                | CatalogStatus::PauseRequested
        ) {
            return None;
        }
        let query = query.trim();
        if query.is_empty() {
            self.generation = self.generation.saturating_add(1);
            self.clear_results();
            self.query.clear();
            self.status = CatalogStatus::Idle;
            return None;
        }
        if query.chars().count() < 2 || query.len() > 128 || query.chars().any(char::is_control) {
            self.generation = self.generation.saturating_add(1);
            self.clear_results();
            self.query = query.into();
            self.status = CatalogStatus::InvalidQuery;
            return None;
        }

        self.generation = self.generation.saturating_add(1);
        self.query = query.into();
        self.clear_results();
        self.status = CatalogStatus::Searching;
        Some(CatalogCommand::Search {
            generation: self.generation,
            query: query.into(),
        })
    }

    pub(crate) fn inspect_repository(&mut self, index: usize) -> Option<CatalogCommand> {
        if !matches!(self.status, CatalogStatus::Repositories) {
            return None;
        }
        let repo = self.repositories.get(index)?.repo.clone();
        self.repositories.clear();
        self.repo = Some(repo.clone());
        self.revision = None;
        self.candidates.clear();
        self.selected = None;
        self.status = CatalogStatus::Inspecting(repo.clone());
        Some(CatalogCommand::Inspect {
            generation: self.generation,
            repo,
        })
    }

    pub(crate) fn select_candidate(&mut self, index: usize) -> bool {
        if !matches!(
            self.status,
            CatalogStatus::Candidates | CatalogStatus::Selected(_)
        ) {
            return false;
        }
        let Some(candidate) = self.candidates.get(index) else {
            return false;
        };
        self.selected = Some(index);
        self.status = CatalogStatus::Selected(candidate.path.clone());
        true
    }

    pub(crate) fn start_transfer(&mut self) -> Option<CatalogCommand> {
        if !self.can_transfer() {
            return None;
        }
        let repo = self.repo.clone()?;
        let revision = self.revision.clone()?;
        let path = self.selected_candidate()?.path.clone();
        self.status = CatalogStatus::Resolving(path.clone());
        Some(CatalogCommand::Transfer {
            generation: self.generation,
            repo,
            revision,
            path,
        })
    }

    pub(crate) fn request_pause(&mut self) -> bool {
        if !self.can_pause() {
            return false;
        }
        self.status = CatalogStatus::PauseRequested;
        true
    }

    pub(crate) fn apply(&mut self, event: CatalogEvent) -> bool {
        if event.generation() != self.generation {
            return false;
        }
        match event {
            CatalogEvent::Repositories { repositories, .. }
                if matches!(self.status, CatalogStatus::Searching) =>
            {
                self.repositories = repositories.into_iter().take(MAX_VISIBLE_RESULTS).collect();
                self.repo = None;
                self.revision = None;
                self.candidates.clear();
                self.selected = None;
                self.status = CatalogStatus::Repositories;
            }
            CatalogEvent::Candidates {
                repo,
                revision,
                candidates,
                ..
            } if matches!(self.status, CatalogStatus::Inspecting(_))
                && self.repo.as_deref() == Some(repo.as_str()) =>
            {
                self.repo = Some(repo);
                self.revision = Some(revision);
                self.candidates = candidates.into_iter().take(MAX_VISIBLE_RESULTS).collect();
                self.selected = None;
                self.status = CatalogStatus::Candidates;
            }
            CatalogEvent::Progress {
                stage,
                transferred_bytes,
                total_bytes,
                ..
            } if matches!(
                self.status,
                CatalogStatus::Resolving(_) | CatalogStatus::Progress { .. }
            ) =>
            {
                self.status = CatalogStatus::Progress {
                    stage,
                    transferred_bytes,
                    total_bytes,
                };
            }
            CatalogEvent::Completed { disposition, .. }
                if matches!(
                    self.status,
                    CatalogStatus::Resolving(_)
                        | CatalogStatus::Progress { .. }
                        | CatalogStatus::PauseRequested
                ) =>
            {
                self.repositories.clear();
                self.candidates.clear();
                self.selected = None;
                self.status = CatalogStatus::Completed(disposition);
            }
            CatalogEvent::Failed { message, .. }
                if !matches!(self.status, CatalogStatus::Completed(_)) =>
            {
                self.repositories.clear();
                self.candidates.clear();
                self.selected = None;
                self.status = CatalogStatus::Error(message);
            }
            _ => return false,
        }
        true
    }

    fn clear_results(&mut self) {
        self.repositories.clear();
        self.repo = None;
        self.revision = None;
        self.candidates.clear();
        self.selected = None;
    }
}

impl CatalogEvent {
    pub(crate) fn generation(&self) -> u64 {
        match self {
            Self::Repositories { generation, .. }
            | Self::Candidates { generation, .. }
            | Self::Progress { generation, .. }
            | Self::Completed { generation, .. }
            | Self::Failed { generation, .. } => *generation,
        }
    }
}

impl TransferStage {
    fn label(self) -> &'static str {
        match self {
            Self::Transferring => "Downloading",
            Self::Verifying => "Verifying",
            Self::Publishing => "Publishing",
        }
    }
}

impl CatalogTransferDisposition {
    fn label(self) -> &'static str {
        match self {
            Self::Installed => "Installed",
            Self::AlreadyInstalled => "Already installed",
            Self::Paused => "Paused",
            Self::Interrupted => "Interrupted",
        }
    }
}

fn format_size(bytes: u64) -> String {
    const MB: f64 = 1_000_000.0;
    const GB: f64 = 1_000_000_000.0;
    if bytes >= 1_000_000_000 {
        format!("{:.1} GB", bytes as f64 / GB)
    } else if bytes >= 1_000_000 {
        format!("{:.1} MB", bytes as f64 / MB)
    } else {
        format!("{bytes} bytes")
    }
}

#[cfg(test)]
mod tests {
    use super::{CandidateItem, CatalogEvent, CatalogState, RepositoryItem};

    fn repositories(count: usize) -> Vec<RepositoryItem> {
        (0..count)
            .map(|index| {
                RepositoryItem::new(
                    format!("owner/model-{index}"),
                    Some(format!("{index} downloads")),
                )
            })
            .collect()
    }

    fn candidates(count: usize) -> Vec<CandidateItem> {
        (0..count)
            .map(|index| CandidateItem::new(format!("model-{index}.gguf"), Some(index as u64 + 1)))
            .collect()
    }

    fn ready_candidates() -> CatalogState {
        ready_with_candidates(candidates(3))
    }

    fn ready_with_candidates(candidates: Vec<CandidateItem>) -> CatalogState {
        let mut state = CatalogState::default();
        let search = state
            .submit_search("models")
            .expect("a valid query starts a search");
        let generation = search.generation();
        assert!(state.apply(CatalogEvent::Repositories {
            generation,
            repositories: repositories(2),
        }));
        let inspect = state
            .inspect_repository(1)
            .expect("a visible repository can be inspected");
        assert!(state.apply(CatalogEvent::Candidates {
            generation: inspect.generation(),
            repo: "owner/model-1".into(),
            revision: "0123456789abcdef0123456789abcdef01234567".into(),
            candidates,
        }));
        state
    }

    #[test]
    fn installed_candidate_keeps_explicit_descriptor_bound_transfer_with_check_action() {
        use super::CatalogCommand;

        let mut installed = ready_with_candidates(vec![CandidateItem::new(
            "model-q4.gguf".into(),
            Some(88_200_000),
        )
        .with_installed_model_id("custom-model".into())]);
        assert!(installed.candidates()[0].is_installed());
        assert_eq!(
            installed.candidates()[0].installed_model_id(),
            Some("custom-model")
        );
        assert!(installed.select_candidate(0));
        assert_eq!(
            installed.transfer_action_label().as_deref(),
            Some("Check installed")
        );
        assert_eq!(
            installed.start_transfer(),
            Some(CatalogCommand::Transfer {
                generation: 1,
                repo: "owner/model-1".into(),
                revision: "0123456789abcdef0123456789abcdef01234567".into(),
                path: "model-q4.gguf".into(),
            })
        );

        let mut ordinary = ready_with_candidates(vec![CandidateItem::new(
            "model-q4.gguf".into(),
            Some(88_200_000),
        )]);
        assert!(!ordinary.candidates()[0].is_installed());
        assert!(ordinary.select_candidate(0));
        assert_eq!(
            ordinary.transfer_action_label().as_deref(),
            Some("Download 88.2 MB")
        );
    }

    #[test]
    fn search_and_inspection_are_bounded_and_never_auto_select() {
        use super::CatalogCommand;

        let mut state = CatalogState::default();

        let search = state
            .submit_search("  small language model  ")
            .expect("trimmed valid input starts a search");
        assert_eq!(
            search,
            CatalogCommand::Search {
                generation: 1,
                query: "small language model".into(),
            }
        );
        assert_eq!(state.status_label(), "Searching Hugging Face…");

        assert!(state.apply(CatalogEvent::Repositories {
            generation: 1,
            repositories: repositories(7),
        }));
        assert_eq!(state.repositories().len(), 5);
        assert_eq!(state.repositories()[4].repo(), "owner/model-4");
        assert_eq!(state.status_label(), "Choose a repository");

        assert_eq!(
            state.inspect_repository(4),
            Some(CatalogCommand::Inspect {
                generation: 1,
                repo: "owner/model-4".into(),
            })
        );
        assert_eq!(state.status_label(), "Inspecting owner/model-4…");

        assert!(state.apply(CatalogEvent::Candidates {
            generation: 1,
            repo: "owner/model-4".into(),
            revision: "0123456789abcdef0123456789abcdef01234567".into(),
            candidates: candidates(7),
        }));
        assert_eq!(state.candidates().len(), 5);
        assert_eq!(state.candidates()[4].path(), "model-4.gguf");
        assert_eq!(state.selected_candidate(), None);
        assert!(!state.can_transfer());
        assert_eq!(state.start_transfer(), None);
        assert_eq!(state.status_label(), "Choose a GGUF file");
    }

    #[test]
    fn compact_browsing_modes_have_one_heading_and_hide_results_during_transfer() {
        use super::CatalogMode;

        let mut state = CatalogState::default();
        assert_eq!(state.mode(), CatalogMode::Idle);
        assert!(!state.owns_main_region());
        assert_eq!(state.browser_heading(), None);

        let generation = state.submit_search("models").unwrap().generation();
        assert_eq!(state.mode(), CatalogMode::Searching);
        assert!(state.owns_main_region());
        assert_eq!(state.browser_heading(), Some("Models"));

        assert!(state.apply(CatalogEvent::Repositories {
            generation,
            repositories: repositories(1),
        }));
        assert_eq!(state.mode(), CatalogMode::Repositories);
        assert_eq!(state.browser_heading(), Some("Models"));
        assert!(state.shows_repository_rows());

        assert!(state.inspect_repository(0).is_some());
        assert_eq!(state.mode(), CatalogMode::Inspecting);
        assert_eq!(state.browser_heading(), Some("Choose a GGUF file"));
        assert_eq!(state.browser_status().as_deref(), Some("Loading files…"));

        assert!(state.apply(CatalogEvent::Candidates {
            generation,
            repo: "owner/model-0".into(),
            revision: "0123456789abcdef0123456789abcdef01234567".into(),
            candidates: vec![CandidateItem::new(
                "a-very-long-model-file-name-q4-k-m.gguf".into(),
                Some(88_200_000),
            )],
        }));
        assert_eq!(state.mode(), CatalogMode::Candidates);
        assert_eq!(state.browser_heading(), Some("Choose a GGUF file"));
        assert!(state.shows_candidate_rows());
        assert!(state.select_candidate(0));
        assert_eq!(state.mode(), CatalogMode::Selected);
        assert_eq!(state.status_label(), "Ready to download");
        assert_eq!(
            state.transfer_action_label().as_deref(),
            Some("Download 88.2 MB")
        );

        assert!(state.start_transfer().is_some());
        assert_eq!(state.mode(), CatalogMode::Transferring);
        assert!(state.owns_main_region());
        assert!(!state.shows_repository_rows());
        assert!(!state.shows_candidate_rows());
    }

    #[test]
    fn terminal_catalog_error_stays_compact_without_owning_the_main_region() {
        use super::CatalogMode;

        let mut state = CatalogState::default();
        let generation = state.submit_search("models").unwrap().generation();
        assert!(state.apply(CatalogEvent::Failed {
            generation,
            message: "Catalog search failed".into(),
        }));

        assert_eq!(state.mode(), CatalogMode::Error);
        assert!(!state.owns_main_region());
        assert_eq!(
            state.browser_status().as_deref(),
            Some("Catalog search failed")
        );
    }

    #[test]
    fn transfer_requires_an_explicit_choice_and_reports_progress_pause_and_already_installed() {
        use super::{CatalogCommand, CatalogTransferDisposition, TransferStage};

        let mut state = ready_candidates();

        assert_eq!(state.start_transfer(), None);
        assert!(state.select_candidate(1));
        assert_eq!(
            state.selected_candidate().map(CandidateItem::path),
            Some("model-1.gguf")
        );
        assert!(state.can_transfer());
        assert_eq!(state.status_label(), "Ready to download");

        let transfer = state
            .start_transfer()
            .expect("the explicitly selected candidate can transfer");
        assert_eq!(
            transfer,
            CatalogCommand::Transfer {
                generation: 1,
                repo: "owner/model-1".into(),
                revision: "0123456789abcdef0123456789abcdef01234567".into(),
                path: "model-1.gguf".into(),
            }
        );
        assert_eq!(state.status_label(), "Preparing model-1.gguf…");

        assert!(state.apply(CatalogEvent::Progress {
            generation: 1,
            stage: TransferStage::Transferring,
            transferred_bytes: 25,
            total_bytes: 100,
        }));
        assert_eq!(state.progress_fraction(), Some(0.25));
        assert!(state.can_pause());
        assert!(state.request_pause());
        assert_eq!(state.status_label(), "Pausing transfer…");

        assert!(state.apply(CatalogEvent::Completed {
            generation: 1,
            disposition: CatalogTransferDisposition::AlreadyInstalled,
        }));
        assert_eq!(state.status_label(), "Already installed");
        assert_eq!(state.progress_fraction(), None);
        assert!(!state.can_pause());
    }

    #[test]
    fn queued_progress_cannot_leave_pause_requested() {
        use super::TransferStage;

        let mut state = ready_candidates();
        assert!(state.select_candidate(0));
        assert!(state.start_transfer().is_some());
        assert!(state.apply(CatalogEvent::Progress {
            generation: 1,
            stage: TransferStage::Transferring,
            transferred_bytes: 25,
            total_bytes: 100,
        }));
        assert!(state.request_pause());

        assert!(!state.apply(CatalogEvent::Progress {
            generation: 1,
            stage: TransferStage::Transferring,
            transferred_bytes: 50,
            total_bytes: 100,
        }));
        assert_eq!(state.status_label(), "Pausing transfer…");
        assert!(!state.can_pause());
    }

    #[test]
    fn stale_generations_cannot_replace_a_newer_search() {
        let mut state = CatalogState::default();
        let old = state.submit_search("old query").unwrap().generation();
        let current = state.submit_search("new query").unwrap().generation();

        assert_eq!((old, current), (1, 2));
        assert!(!state.apply(CatalogEvent::Repositories {
            generation: old,
            repositories: repositories(5),
        }));
        assert!(!state.apply(CatalogEvent::Failed {
            generation: old,
            message: "old request failed".into(),
        }));
        assert_eq!(state.query(), "new query");
        assert!(state.repositories().is_empty());
        assert_eq!(state.status_label(), "Searching Hugging Face…");

        assert!(state.apply(CatalogEvent::Repositories {
            generation: current,
            repositories: repositories(1),
        }));
        assert_eq!(state.repositories()[0].repo(), "owner/model-0");
    }

    #[test]
    fn short_or_empty_edits_stay_local_and_invalidate_older_results_and_errors() {
        use super::CatalogCommand;

        let mut state = CatalogState::default();
        let first = state
            .submit_search("old query")
            .expect("two or more characters dispatch")
            .generation();

        assert_eq!(state.submit_search("x"), None);
        assert_eq!(state.query(), "x");
        assert_eq!(state.status_label(), "Type at least 2 characters");
        assert_eq!(state.generation(), first + 1);
        assert!(!state.apply(CatalogEvent::Repositories {
            generation: first,
            repositories: repositories(1),
        }));
        assert!(state.repositories().is_empty());

        let second = state
            .submit_search("new query")
            .expect("a later valid edit dispatches")
            .generation();
        assert_eq!(state.submit_search("   "), None);
        assert_eq!(state.query(), "");
        assert_eq!(state.status_label(), "Search Hugging Face");
        assert_eq!(state.generation(), second + 1);
        assert!(!state.apply(CatalogEvent::Failed {
            generation: second,
            message: "stale failure".into(),
        }));
        assert_eq!(state.status_label(), "Search Hugging Face");

        assert_eq!(
            state.submit_search("go"),
            Some(CatalogCommand::Search {
                generation: second + 2,
                query: "go".into(),
            })
        );
        assert_eq!(state.query(), "go");
    }

    #[test]
    fn invalid_search_is_a_deterministic_local_error() {
        let mut state = CatalogState::default();

        assert_eq!(state.submit_search(" x "), None);
        assert_eq!(state.generation(), 1);
        assert_eq!(state.query(), "x");
        assert_eq!(state.status_label(), "Type at least 2 characters");
        assert!(state.repositories().is_empty());
        assert!(state.candidates().is_empty());
    }

    #[test]
    fn every_transfer_stage_and_terminal_disposition_has_a_truthful_status() {
        use super::{CatalogTransferDisposition, TransferStage};

        for (stage, expected) in [
            (TransferStage::Transferring, "Downloading 1 of 2 bytes"),
            (TransferStage::Verifying, "Verifying 1 of 2 bytes"),
            (TransferStage::Publishing, "Publishing 1 of 2 bytes"),
        ] {
            let mut state = ready_candidates();
            assert!(state.select_candidate(0));
            assert!(state.start_transfer().is_some());
            assert!(state.apply(CatalogEvent::Progress {
                generation: 1,
                stage,
                transferred_bytes: 1,
                total_bytes: 2,
            }));
            assert_eq!(state.status_label(), expected);
        }

        for (disposition, expected) in [
            (CatalogTransferDisposition::Installed, "Installed"),
            (
                CatalogTransferDisposition::AlreadyInstalled,
                "Already installed",
            ),
            (CatalogTransferDisposition::Paused, "Paused"),
            (CatalogTransferDisposition::Interrupted, "Interrupted"),
        ] {
            let mut state = ready_candidates();
            assert!(state.select_candidate(0));
            assert!(state.start_transfer().is_some());
            assert!(state.apply(CatalogEvent::Completed {
                generation: 1,
                disposition,
            }));
            assert_eq!(state.status_label(), expected);
        }
    }
}
