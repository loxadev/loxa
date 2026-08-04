#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Bundle {
    Absent,
    Partial,
    Verified { target_bytes: u64, draft_bytes: u64 },
    Recovery(RecoveryReason),
}

impl Bundle {
    pub(crate) fn verified(target_bytes: u64, draft_bytes: u64) -> Self {
        Self::Verified {
            target_bytes,
            draft_bytes,
        }
    }

    pub(crate) fn recovery(reason: RecoveryReason) -> Self {
        Self::Recovery(reason)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryReason {
    Invalid,
    Busy,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Recommendation {
    Available {
        target_bytes: u64,
        draft_bytes: u64,
    },
    Unavailable {
        reason: RecommendationUnavailableReason,
        target_bytes: u64,
        draft_bytes: u64,
    },
    Hidden,
}

impl Recommendation {
    pub(crate) fn available(target_bytes: u64, draft_bytes: u64) -> Self {
        Self::Available {
            target_bytes,
            draft_bytes,
        }
    }

    pub(crate) fn unavailable(
        reason: RecommendationUnavailableReason,
        target_bytes: u64,
        draft_bytes: u64,
    ) -> Self {
        Self::Unavailable {
            reason,
            target_bytes,
            draft_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecommendationUnavailableReason {
    InsufficientMemory,
    InsufficientDisk,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Download {
    Idle,
    Active(TransferProgress),
    Paused(TransferProgress),
    Failed(TransferProgress),
}

impl Download {
    pub(crate) fn active(completed_bytes: u64, total_bytes: u64, phase: DownloadPhase) -> Self {
        Self::Active(TransferProgress {
            completed_bytes,
            total_bytes,
            phase,
        })
    }

    pub(crate) fn paused(completed_bytes: u64, total_bytes: u64, phase: DownloadPhase) -> Self {
        Self::Paused(TransferProgress {
            completed_bytes,
            total_bytes,
            phase,
        })
    }

    pub(crate) fn failed(completed_bytes: u64, total_bytes: u64, phase: DownloadPhase) -> Self {
        Self::Failed(TransferProgress {
            completed_bytes,
            total_bytes,
            phase,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DownloadPhase {
    Preparing,
    Target,
    Draft,
    Verifying,
    Publishing,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TransferProgress {
    completed_bytes: u64,
    total_bytes: u64,
    phase: DownloadPhase,
}

pub(crate) struct MenuLayout;

impl MenuLayout {
    pub(crate) const BASE_WIDTH: f64 = 300.0;
    pub(crate) const MAX_WIDTH: f64 = 400.0;
    pub(crate) const OUTER_PADDING: f64 = 5.0;
    pub(crate) const INNER_PADDING: f64 = 8.0;
    pub(crate) const VERTICAL_PADDING: f64 = 4.0;
    pub(crate) const HOVER_RADIUS: f64 = 6.0;
    pub(crate) const ICON_CONTAINER: f64 = 28.0;
    pub(crate) const MODEL_ROW_HEIGHT: f64 = 40.0;
    pub(crate) const TRANSFER_ROW_HEIGHT: f64 = 116.0;
    pub(crate) const PRIMARY_FONT_SIZE: f64 = 13.0;
    pub(crate) const SECONDARY_FONT_SIZE: f64 = 11.0;

    pub(crate) fn width_for(ideal_width: f64) -> f64 {
        ideal_width.clamp(Self::BASE_WIDTH, Self::MAX_WIDTH)
    }

    pub(crate) fn content_width(menu_width: f64) -> f64 {
        menu_width - 2.0 * (Self::OUTER_PADDING + Self::INNER_PADDING)
    }

    pub(crate) fn model_row_height() -> f64 {
        Self::MODEL_ROW_HEIGHT
    }

    pub(crate) fn transfer_row_height() -> f64 {
        Self::TRANSFER_ROW_HEIGHT
    }

    pub(crate) fn icon_container() -> f64 {
        Self::ICON_CONTAINER
    }

    pub(crate) fn primary_font_size() -> f64 {
        Self::PRIMARY_FONT_SIZE
    }

    pub(crate) fn secondary_font_size() -> f64 {
        Self::SECONDARY_FONT_SIZE
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Runtime {
    Idle,
    Starting,
    Running,
    Stopping,
    Error,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeInventory {
    Managed,
    Missing,
    External,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MenuAction {
    Start,
    Pause,
    Resume,
    Retry,
    Cancel,
    KeepPartial,
    DiscardPartial,
}

impl MenuAction {
    pub(crate) fn confirmation_label(self) -> Option<&'static str> {
        match self {
            Self::Cancel => Some("Cancel"),
            Self::KeepPartial => Some("Keep partial"),
            Self::DiscardPartial => Some("Discard partial"),
            Self::Start | Self::Pause | Self::Resume | Self::Retry => None,
        }
    }

    pub(crate) fn accessibility_label(self) -> Option<&'static str> {
        match self {
            Self::Cancel => Some("Cancel download"),
            Self::KeepPartial => Some("Keep partial download"),
            Self::DiscardPartial => Some("Discard partial download"),
            Self::Start | Self::Pause | Self::Resume | Self::Retry => None,
        }
    }
}

#[derive(Default)]
pub(crate) struct InlineCancelState {
    confirming: bool,
}

impl InlineCancelState {
    pub(crate) fn activate_cancel(&mut self) {
        self.confirming = true;
    }

    pub(crate) fn keep_partial(&mut self) {
        self.reset();
    }

    pub(crate) fn discard_partial(&mut self) -> bool {
        let was_confirming = self.confirming;
        self.reset();
        was_confirming
    }

    pub(crate) fn reset(&mut self) {
        self.confirming = false;
    }

    pub(crate) fn visible_actions(&self) -> Vec<MenuAction> {
        if self.confirming {
            vec![MenuAction::KeepPartial, MenuAction::DiscardPartial]
        } else {
            vec![MenuAction::Cancel]
        }
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MenuSection {
    Header,
    InstalledEmpty,
    InstalledModel,
    Recovery,
    Downloading,
    Recommendation,
    Footer,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RecommendationRow {
    target_bytes: u64,
    draft_bytes: u64,
    availability: RecommendationAvailability,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RecommendationAvailability {
    Eligible,
    Unavailable(RecommendationUnavailableReason),
}

impl RecommendationRow {
    pub(crate) fn action(&self) -> Option<MenuAction> {
        matches!(self.availability, RecommendationAvailability::Eligible)
            .then_some(MenuAction::Start)
    }

    pub(crate) fn subtitle(&self) -> String {
        format!(
            "12B · Q4_K_M · MTP · {}",
            format_human_size(self.target_bytes.saturating_add(self.draft_bytes))
        )
    }

    pub(crate) fn size_detail(&self) -> String {
        format!(
            "{} bytes",
            format_bytes(self.target_bytes.saturating_add(self.draft_bytes))
        )
    }

    pub(crate) fn disabled_reason(&self) -> Option<&'static str> {
        match self.availability {
            RecommendationAvailability::Eligible => None,
            RecommendationAvailability::Unavailable(
                RecommendationUnavailableReason::InsufficientMemory,
            ) => Some("Insufficient memory"),
            RecommendationAvailability::Unavailable(
                RecommendationUnavailableReason::InsufficientDisk,
            ) => Some("Insufficient disk space"),
            RecommendationAvailability::Unavailable(
                RecommendationUnavailableReason::Unavailable,
            ) => Some("Unavailable on this Mac"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct InstalledRow {
    target_bytes: u64,
    draft_bytes: u64,
    active_runtime: bool,
}

impl InstalledRow {
    pub(crate) fn verification_label(&self) -> &'static str {
        "Verified"
    }

    pub(crate) fn subtitle(&self) -> String {
        format!(
            "Q4_K_M · MTP · {}",
            format_human_size(self.target_bytes.saturating_add(self.draft_bytes))
        )
    }

    pub(crate) fn size_detail(&self) -> String {
        format!(
            "{} bytes",
            format_bytes(self.target_bytes.saturating_add(self.draft_bytes))
        )
    }

    pub(crate) fn runtime_note(&self) -> Option<&'static str> {
        self.active_runtime.then_some("Active runtime")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Footer {
    inventory: RuntimeInventory,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RecoveryRow {
    reason: RecoveryReason,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TransferState {
    Active,
    Paused,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TransferRow {
    state: TransferState,
    progress: TransferProgress,
}

impl TransferRow {
    pub(crate) fn primary_action(&self) -> MenuAction {
        match self.state {
            TransferState::Active => MenuAction::Pause,
            TransferState::Paused => MenuAction::Resume,
            TransferState::Failed => MenuAction::Retry,
        }
    }

    pub(crate) fn phase_label(&self) -> &'static str {
        match (self.state, self.progress.phase) {
            (TransferState::Active, DownloadPhase::Preparing) => "Preparing",
            (TransferState::Active, DownloadPhase::Target) => "Downloading target",
            (TransferState::Active, DownloadPhase::Draft) => "Downloading MTP draft",
            (TransferState::Active, DownloadPhase::Verifying) => "Verifying",
            (TransferState::Active, DownloadPhase::Publishing) => "Publishing",
            (TransferState::Paused, DownloadPhase::Preparing) => "Paused during preparation",
            (TransferState::Paused, DownloadPhase::Target) => "Paused during target download",
            (TransferState::Paused, DownloadPhase::Draft) => "Paused during MTP draft",
            (TransferState::Paused, DownloadPhase::Verifying) => "Paused during verification",
            (TransferState::Paused, DownloadPhase::Publishing) => "Paused during publishing",
            (TransferState::Failed, DownloadPhase::Preparing) => "Preparation failed",
            (TransferState::Failed, DownloadPhase::Target) => "Target download failed",
            (TransferState::Failed, DownloadPhase::Draft) => "MTP draft download failed",
            (TransferState::Failed, DownloadPhase::Verifying) => "Verification failed",
            (TransferState::Failed, DownloadPhase::Publishing) => "Publishing failed",
        }
    }

    pub(crate) fn progress_text(&self) -> String {
        format!(
            "{} of {}",
            format_human_size(self.progress.completed_bytes),
            format_human_size(self.progress.total_bytes)
        )
    }

    pub(crate) fn progress_detail(&self) -> String {
        format!(
            "{} of {} bytes",
            format_bytes(self.progress.completed_bytes),
            format_bytes(self.progress.total_bytes)
        )
    }

    pub(crate) fn progress_fraction(&self) -> f64 {
        self.progress.completed_bytes as f64 / self.progress.total_bytes as f64
    }
}

impl RecoveryRow {
    pub(crate) fn detail(&self) -> &'static str {
        match self.reason {
            RecoveryReason::Invalid => "Verify the managed bundle before trying again.",
            RecoveryReason::Busy => "Another Loxa process is updating this bundle.",
        }
    }
}

impl Footer {
    pub(crate) fn label(&self, version: &str) -> String {
        format!("Loxa {version}")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum MenuBody {
    CleanAbsence(RecommendationRow),
    Verified(InstalledRow),
    Recovery(RecoveryRow),
    Partial(TransferRow),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MenuSnapshot {
    body: MenuBody,
    runtime: Runtime,
    footer: Footer,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MenuUpdate {
    Rebuild,
    UpdateRetainedRows,
}

const FIXTURE_TARGET_BYTES: u64 = 6_716_356_800;
const FIXTURE_DRAFT_BYTES: u64 = 253_708_800;
const FIXTURE_TOTAL_BYTES: u64 = FIXTURE_TARGET_BYTES + FIXTURE_DRAFT_BYTES;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Fixture {
    Empty,
    LowMemory,
    LowDisk,
    Unavailable,
    Installed,
    Completed,
    Running,
    Invalid,
    Busy,
    Preparing,
    Downloading,
    Draft,
    Verifying,
    Publishing,
    Paused,
    Failed,
    Starting,
    Stopping,
    Error,
}

impl Fixture {
    pub(crate) fn parse(name: &str) -> Option<Self> {
        match name {
            "empty" => Some(Self::Empty),
            "low-memory" => Some(Self::LowMemory),
            "low-disk" => Some(Self::LowDisk),
            "unavailable" => Some(Self::Unavailable),
            "installed" => Some(Self::Installed),
            "completed" => Some(Self::Completed),
            "running" => Some(Self::Running),
            "invalid" => Some(Self::Invalid),
            "busy" => Some(Self::Busy),
            "preparing" => Some(Self::Preparing),
            "downloading" => Some(Self::Downloading),
            "draft" => Some(Self::Draft),
            "verifying" => Some(Self::Verifying),
            "publishing" => Some(Self::Publishing),
            "paused" => Some(Self::Paused),
            "failed" => Some(Self::Failed),
            "starting" => Some(Self::Starting),
            "stopping" => Some(Self::Stopping),
            "error" => Some(Self::Error),
            _ => None,
        }
    }

    pub(crate) fn snapshot(self) -> MenuSnapshot {
        let (bundle, recommendation, download, runtime, inventory) = match self {
            Self::Empty => (
                Bundle::Absent,
                Recommendation::available(FIXTURE_TARGET_BYTES, FIXTURE_DRAFT_BYTES),
                Download::Idle,
                Runtime::Idle,
                RuntimeInventory::Missing,
            ),
            Self::LowMemory => (
                Bundle::Absent,
                Recommendation::unavailable(
                    RecommendationUnavailableReason::InsufficientMemory,
                    FIXTURE_TARGET_BYTES,
                    FIXTURE_DRAFT_BYTES,
                ),
                Download::Idle,
                Runtime::Idle,
                RuntimeInventory::Missing,
            ),
            Self::LowDisk => (
                Bundle::Absent,
                Recommendation::unavailable(
                    RecommendationUnavailableReason::InsufficientDisk,
                    FIXTURE_TARGET_BYTES,
                    FIXTURE_DRAFT_BYTES,
                ),
                Download::Idle,
                Runtime::Idle,
                RuntimeInventory::Missing,
            ),
            Self::Unavailable => (
                Bundle::Absent,
                Recommendation::unavailable(
                    RecommendationUnavailableReason::Unavailable,
                    FIXTURE_TARGET_BYTES,
                    FIXTURE_DRAFT_BYTES,
                ),
                Download::Idle,
                Runtime::Idle,
                RuntimeInventory::Missing,
            ),
            Self::Installed => (
                Bundle::verified(FIXTURE_TARGET_BYTES, FIXTURE_DRAFT_BYTES),
                Recommendation::Hidden,
                Download::Idle,
                Runtime::Idle,
                RuntimeInventory::Managed,
            ),
            Self::Completed => (
                Bundle::verified(FIXTURE_TARGET_BYTES, FIXTURE_DRAFT_BYTES),
                Recommendation::Hidden,
                Download::Idle,
                Runtime::Idle,
                RuntimeInventory::Missing,
            ),
            Self::Running => (
                Bundle::verified(FIXTURE_TARGET_BYTES, FIXTURE_DRAFT_BYTES),
                Recommendation::Hidden,
                Download::Idle,
                Runtime::Running,
                RuntimeInventory::External,
            ),
            Self::Invalid => (
                Bundle::recovery(RecoveryReason::Invalid),
                Recommendation::Hidden,
                Download::Idle,
                Runtime::Error,
                RuntimeInventory::Missing,
            ),
            Self::Busy => (
                Bundle::recovery(RecoveryReason::Busy),
                Recommendation::Hidden,
                Download::Idle,
                Runtime::Idle,
                RuntimeInventory::Missing,
            ),
            Self::Preparing => partial_fixture(Download::active(
                0,
                FIXTURE_TOTAL_BYTES,
                DownloadPhase::Preparing,
            )),
            Self::Downloading => partial_fixture(Download::active(
                1_234_567_890,
                FIXTURE_TOTAL_BYTES,
                DownloadPhase::Target,
            )),
            Self::Draft => partial_fixture(Download::active(
                6_800_000_000,
                FIXTURE_TOTAL_BYTES,
                DownloadPhase::Draft,
            )),
            Self::Verifying => partial_fixture(Download::active(
                FIXTURE_TOTAL_BYTES,
                FIXTURE_TOTAL_BYTES,
                DownloadPhase::Verifying,
            )),
            Self::Publishing => partial_fixture(Download::active(
                FIXTURE_TOTAL_BYTES,
                FIXTURE_TOTAL_BYTES,
                DownloadPhase::Publishing,
            )),
            Self::Paused => partial_fixture(Download::paused(
                2_345_678_901,
                FIXTURE_TOTAL_BYTES,
                DownloadPhase::Draft,
            )),
            Self::Failed => partial_fixture(Download::failed(
                3_456_789_012,
                FIXTURE_TOTAL_BYTES,
                DownloadPhase::Verifying,
            )),
            Self::Starting => (
                Bundle::Absent,
                Recommendation::available(FIXTURE_TARGET_BYTES, FIXTURE_DRAFT_BYTES),
                Download::Idle,
                Runtime::Starting,
                RuntimeInventory::Missing,
            ),
            Self::Stopping => (
                Bundle::Absent,
                Recommendation::available(FIXTURE_TARGET_BYTES, FIXTURE_DRAFT_BYTES),
                Download::Idle,
                Runtime::Stopping,
                RuntimeInventory::Missing,
            ),
            Self::Error => (
                Bundle::Absent,
                Recommendation::available(FIXTURE_TARGET_BYTES, FIXTURE_DRAFT_BYTES),
                Download::Idle,
                Runtime::Error,
                RuntimeInventory::Missing,
            ),
        };

        MenuSnapshot::new(bundle, recommendation, download, runtime, inventory)
            .expect("every built-in fixture is canonical")
    }
}

fn partial_fixture(
    download: Download,
) -> (Bundle, Recommendation, Download, Runtime, RuntimeInventory) {
    (
        Bundle::Partial,
        Recommendation::Hidden,
        download,
        Runtime::Idle,
        RuntimeInventory::Missing,
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MenuComposition {
    CleanEligible,
    CleanUnavailable,
    Installed,
    Recovery,
    ActiveTransfer,
    PausedTransfer,
    FailedTransfer,
}

impl MenuSnapshot {
    pub(crate) fn new(
        bundle: Bundle,
        recommendation: Recommendation,
        download: Download,
        runtime: Runtime,
        runtime_inventory: RuntimeInventory,
    ) -> Result<Self, &'static str> {
        let body = match (bundle, recommendation, download) {
            (
                Bundle::Absent,
                Recommendation::Available {
                    target_bytes,
                    draft_bytes,
                },
                Download::Idle,
            ) => MenuBody::CleanAbsence(RecommendationRow {
                target_bytes,
                draft_bytes,
                availability: RecommendationAvailability::Eligible,
            }),
            (
                Bundle::Absent,
                Recommendation::Unavailable {
                    reason,
                    target_bytes,
                    draft_bytes,
                },
                Download::Idle,
            ) => MenuBody::CleanAbsence(RecommendationRow {
                target_bytes,
                draft_bytes,
                availability: RecommendationAvailability::Unavailable(reason),
            }),
            (
                Bundle::Verified {
                    target_bytes,
                    draft_bytes,
                },
                Recommendation::Hidden,
                Download::Idle,
            ) => MenuBody::Verified(InstalledRow {
                target_bytes,
                draft_bytes,
                active_runtime: matches!(runtime, Runtime::Running),
            }),
            (Bundle::Recovery(reason), Recommendation::Hidden, Download::Idle) => {
                MenuBody::Recovery(RecoveryRow { reason })
            }
            (Bundle::Partial, Recommendation::Hidden, Download::Active(progress)) => {
                MenuBody::Partial(transfer_row(TransferState::Active, progress)?)
            }
            (Bundle::Partial, Recommendation::Hidden, Download::Paused(progress)) => {
                MenuBody::Partial(transfer_row(TransferState::Paused, progress)?)
            }
            (Bundle::Partial, Recommendation::Hidden, Download::Failed(progress)) => {
                MenuBody::Partial(transfer_row(TransferState::Failed, progress)?)
            }
            _ => return Err("snapshot combination is not canonical"),
        };

        Ok(Self {
            body,
            runtime,
            footer: Footer {
                inventory: runtime_inventory,
            },
        })
    }

    #[cfg(test)]
    pub(crate) fn section_kinds(&self) -> Vec<MenuSection> {
        match self.body {
            MenuBody::CleanAbsence(_) => vec![
                MenuSection::Header,
                MenuSection::InstalledEmpty,
                MenuSection::Recommendation,
                MenuSection::Footer,
            ],
            MenuBody::Verified(_) => vec![
                MenuSection::Header,
                MenuSection::InstalledModel,
                MenuSection::Footer,
            ],
            MenuBody::Recovery(_) => vec![
                MenuSection::Header,
                MenuSection::Recovery,
                MenuSection::Footer,
            ],
            MenuBody::Partial(_) => vec![
                MenuSection::Header,
                MenuSection::Downloading,
                MenuSection::Footer,
            ],
        }
    }

    pub(crate) fn recommendation_row(&self) -> Option<&RecommendationRow> {
        match &self.body {
            MenuBody::CleanAbsence(row) => Some(row),
            MenuBody::Verified(_) | MenuBody::Recovery(_) | MenuBody::Partial(_) => None,
        }
    }

    pub(crate) fn installed_row(&self) -> Option<&InstalledRow> {
        match &self.body {
            MenuBody::CleanAbsence(_) => None,
            MenuBody::Verified(row) => Some(row),
            MenuBody::Recovery(_) | MenuBody::Partial(_) => None,
        }
    }

    pub(crate) fn recovery_row(&self) -> Option<&RecoveryRow> {
        match &self.body {
            MenuBody::Recovery(row) => Some(row),
            MenuBody::CleanAbsence(_) | MenuBody::Verified(_) | MenuBody::Partial(_) => None,
        }
    }

    pub(crate) fn transfer_row(&self) -> Option<&TransferRow> {
        match &self.body {
            MenuBody::Partial(row) => Some(row),
            MenuBody::CleanAbsence(_) | MenuBody::Verified(_) | MenuBody::Recovery(_) => None,
        }
    }

    pub(crate) fn footer(&self) -> &Footer {
        &self.footer
    }

    pub(crate) fn runtime_label(&self) -> &'static str {
        match self.runtime {
            Runtime::Idle => "Runtime: Stopped",
            Runtime::Starting => "Runtime: Starting",
            Runtime::Running => "Runtime: Running",
            Runtime::Stopping => "Runtime: Stopping",
            Runtime::Error => "Runtime: Error",
        }
    }

    pub(crate) fn update_from(&self, previous: Option<&Self>) -> MenuUpdate {
        if previous.is_some_and(|previous| previous.composition() == self.composition()) {
            MenuUpdate::UpdateRetainedRows
        } else {
            MenuUpdate::Rebuild
        }
    }

    pub(crate) fn apply_fixture_action(&self, action: MenuAction) -> Option<Self> {
        let download = match (&self.body, action) {
            (MenuBody::CleanAbsence(row), MenuAction::Start)
                if matches!(row.availability, RecommendationAvailability::Eligible) =>
            {
                Download::active(
                    0,
                    row.target_bytes.saturating_add(row.draft_bytes),
                    DownloadPhase::Preparing,
                )
            }
            (MenuBody::Partial(row), MenuAction::Pause) if row.state == TransferState::Active => {
                Download::paused(
                    row.progress.completed_bytes,
                    row.progress.total_bytes,
                    row.progress.phase,
                )
            }
            (MenuBody::Partial(row), MenuAction::Resume) if row.state == TransferState::Paused => {
                Download::active(
                    row.progress.completed_bytes,
                    row.progress.total_bytes,
                    row.progress.phase,
                )
            }
            (MenuBody::Partial(row), MenuAction::Retry) if row.state == TransferState::Failed => {
                Download::active(
                    row.progress.completed_bytes,
                    row.progress.total_bytes,
                    row.progress.phase,
                )
            }
            _ => return None,
        };

        Self::new(
            Bundle::Partial,
            Recommendation::Hidden,
            download,
            self.runtime,
            self.footer.inventory,
        )
        .ok()
    }
    fn composition(&self) -> MenuComposition {
        match &self.body {
            MenuBody::CleanAbsence(row) => match row.availability {
                RecommendationAvailability::Eligible => MenuComposition::CleanEligible,
                RecommendationAvailability::Unavailable(_) => MenuComposition::CleanUnavailable,
            },
            MenuBody::Verified(_) => MenuComposition::Installed,
            MenuBody::Recovery(_) => MenuComposition::Recovery,
            MenuBody::Partial(row) => match row.state {
                TransferState::Active => MenuComposition::ActiveTransfer,
                TransferState::Paused => MenuComposition::PausedTransfer,
                TransferState::Failed => MenuComposition::FailedTransfer,
            },
        }
    }
}

fn transfer_row(
    state: TransferState,
    progress: TransferProgress,
) -> Result<TransferRow, &'static str> {
    if progress.total_bytes == 0 || progress.completed_bytes > progress.total_bytes {
        return Err("transfer progress must stay within its exact total");
    }
    Ok(TransferRow { state, progress })
}

fn format_bytes(bytes: u64) -> String {
    let digits = bytes.to_string();
    let mut output = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, digit) in digits.chars().enumerate() {
        if index != 0 && (digits.len() - index).is_multiple_of(3) {
            output.push(',');
        }
        output.push(digit);
    }
    output
}

fn format_human_size(bytes: u64) -> String {
    const KB: f64 = 1_000.0;
    const MB: f64 = KB * 1_000.0;
    const GB: f64 = MB * 1_000.0;

    match bytes {
        0 => "0 B".to_owned(),
        bytes if (bytes as f64) < MB => format!("{:.0} KB", bytes as f64 / KB),
        bytes if (bytes as f64) < GB => format!("{:.1} MB", bytes as f64 / MB),
        _ => format!("{:.1} GB", bytes as f64 / GB),
    }
}
