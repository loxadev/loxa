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
        sizes: Option<(u64, u64)>,
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

    #[cfg(test)]
    pub(crate) fn unavailable(
        reason: RecommendationUnavailableReason,
        target_bytes: u64,
        draft_bytes: u64,
    ) -> Self {
        Self::Unavailable {
            reason,
            sizes: Some((target_bytes, draft_bytes)),
        }
    }

    pub(crate) fn unavailable_without_size(reason: RecommendationUnavailableReason) -> Self {
        Self::Unavailable {
            reason,
            sizes: None,
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
    Paused(TransferProgress),
    #[cfg(test)]
    Active(FixtureTransferProgress),
    #[cfg(test)]
    FixturePaused(FixtureTransferProgress),
    #[cfg(test)]
    Failed(FixtureTransferProgress),
}

impl Download {
    #[cfg(test)]
    pub(crate) fn active(completed_bytes: u64, total_bytes: u64, phase: DownloadPhase) -> Self {
        Self::Active(FixtureTransferProgress::new(
            completed_bytes,
            total_bytes,
            phase,
        ))
    }

    pub(crate) fn paused(completed_bytes: u64, total_bytes: u64) -> Self {
        Self::Paused(TransferProgress {
            completed_bytes,
            total_bytes,
        })
    }

    #[cfg(test)]
    pub(crate) fn fixture_paused(
        completed_bytes: u64,
        total_bytes: u64,
        phase: DownloadPhase,
    ) -> Self {
        Self::FixturePaused(FixtureTransferProgress::new(
            completed_bytes,
            total_bytes,
            phase,
        ))
    }

    #[cfg(test)]
    pub(crate) fn failed(completed_bytes: u64, total_bytes: u64, phase: DownloadPhase) -> Self {
        Self::Failed(FixtureTransferProgress::new(
            completed_bytes,
            total_bytes,
            phase,
        ))
    }
}

#[cfg(test)]
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
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FixtureTransferProgress {
    progress: TransferProgress,
    phase: DownloadPhase,
}

#[cfg(test)]
impl FixtureTransferProgress {
    fn new(completed_bytes: u64, total_bytes: u64, phase: DownloadPhase) -> Self {
        Self {
            progress: TransferProgress {
                completed_bytes,
                total_bytes,
            },
            phase,
        }
    }
}

pub(crate) struct MenuLayout;

impl MenuLayout {
    pub(crate) const BASE_WIDTH: f64 = 360.0;
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
    #[cfg(test)]
    Managed,
    Missing,
    External,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ObservedRuntimeOwner {
    Legacy,
    Foreground,
    PersistentApp,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ObservedRuntime {
    owner: ObservedRuntimeOwner,
    model_id: String,
    port: u16,
}

impl ObservedRuntime {
    pub(crate) fn new(
        owner: ObservedRuntimeOwner,
        model_id: String,
        port: u16,
    ) -> Result<Self, &'static str> {
        if model_id.is_empty() || port == 0 {
            return Err("observed runtime requires a model and nonzero port");
        }
        Ok(Self {
            owner,
            model_id,
            port,
        })
    }

    pub(crate) fn owner(&self) -> ObservedRuntimeOwner {
        self.owner
    }

    pub(crate) fn model_id(&self) -> &str {
        &self.model_id
    }

    pub(crate) fn port(&self) -> u16 {
        self.port
    }
}

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
#[derive(Default)]
pub(crate) struct InlineCancelState {
    confirming: bool,
}

#[cfg(test)]
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
    sizes: Option<(u64, u64)>,
    quantization: Option<&'static str>,
    availability: RecommendationAvailability,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RecommendationAvailability {
    Eligible,
    Unavailable(RecommendationUnavailableReason),
}

impl RecommendationRow {
    #[cfg(test)]
    pub(crate) fn action(&self) -> Option<MenuAction> {
        matches!(self.availability, RecommendationAvailability::Eligible)
            .then_some(MenuAction::Start)
    }

    pub(crate) fn subtitle(&self) -> Option<String> {
        self.sizes.map(|(target_bytes, draft_bytes)| {
            let size = format_human_size(target_bytes.saturating_add(draft_bytes));
            let quantization = quantization_segment(self.quantization);
            format!("12B · {quantization}MTP · {size}")
        })
    }

    pub(crate) fn size_detail(&self) -> Option<String> {
        self.sizes.map(|(target_bytes, draft_bytes)| {
            format!(
                "{} bytes",
                format_bytes(target_bytes.saturating_add(draft_bytes))
            )
        })
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
    quantization: Option<&'static str>,
    active_runtime: bool,
}

impl InstalledRow {
    pub(crate) fn verification_label(&self) -> &'static str {
        "Verified"
    }

    pub(crate) fn subtitle(&self) -> String {
        let size = format_human_size(self.target_bytes.saturating_add(self.draft_bytes));
        let quantization = quantization_segment(self.quantization);
        format!("{quantization}MTP · {size}")
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
    inventory: Option<RuntimeInventory>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RecoveryRow {
    reason: RecoveryReason,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TransferState {
    Paused,
    #[cfg(test)]
    Active,
    #[cfg(test)]
    FixturePaused,
    #[cfg(test)]
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TransferRow {
    state: TransferState,
    progress: TransferProgress,
    #[cfg(test)]
    phase: Option<DownloadPhase>,
}

impl TransferRow {
    #[cfg(test)]
    pub(crate) fn primary_action(&self) -> MenuAction {
        match self.state {
            TransferState::Active => MenuAction::Pause,
            TransferState::FixturePaused => MenuAction::Resume,
            TransferState::Failed => MenuAction::Retry,
            TransferState::Paused => unreachable!("live paused observations have no fake action"),
        }
    }

    #[cfg(test)]
    pub(crate) fn has_fixture_action(&self) -> bool {
        !matches!(self.state, TransferState::Paused)
    }

    pub(crate) fn phase_label(&self) -> &'static str {
        match self.state {
            TransferState::Paused => "Paused",
            #[cfg(test)]
            TransferState::Active => match self.phase.expect("fixtures always have a phase") {
                DownloadPhase::Preparing => "Preparing",
                DownloadPhase::Target => "Downloading target",
                DownloadPhase::Draft => "Downloading MTP draft",
                DownloadPhase::Verifying => "Verifying",
                DownloadPhase::Publishing => "Publishing",
            },
            #[cfg(test)]
            TransferState::FixturePaused => match self.phase.expect("fixtures always have a phase")
            {
                DownloadPhase::Preparing => "Paused during preparation",
                DownloadPhase::Target => "Paused during target download",
                DownloadPhase::Draft => "Paused during MTP draft",
                DownloadPhase::Verifying => "Paused during verification",
                DownloadPhase::Publishing => "Paused during publishing",
            },
            #[cfg(test)]
            TransferState::Failed => match self.phase.expect("fixtures always have a phase") {
                DownloadPhase::Preparing => "Preparation failed",
                DownloadPhase::Target => "Target download failed",
                DownloadPhase::Draft => "MTP draft download failed",
                DownloadPhase::Verifying => "Verification failed",
                DownloadPhase::Publishing => "Publishing failed",
            },
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
    pub(crate) fn title(&self) -> &'static str {
        match self.reason {
            RecoveryReason::Invalid => "Managed bundle needs recovery",
            RecoveryReason::Busy => "Managed bundle is in use",
        }
    }

    pub(crate) fn detail(&self) -> &'static str {
        match self.reason {
            RecoveryReason::Invalid => "Verify the managed bundle before trying again.",
            RecoveryReason::Busy => "A Loxa runtime or model operation is using this bundle.",
        }
    }

    pub(crate) fn is_busy(&self) -> bool {
        self.reason == RecoveryReason::Busy
    }
}

impl Footer {
    pub(crate) fn label(&self, version: &str) -> String {
        format!("Loxa {version}")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum MenuBody {
    Loading,
    Error(String),
    CleanAbsence(RecommendationRow),
    Verified(InstalledRow),
    Recovery(RecoveryRow),
    Partial(TransferRow),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MenuSnapshot {
    body: MenuBody,
    runtime: Option<Runtime>,
    observed_runtime: Option<ObservedRuntime>,
    bundle_model_id: Option<String>,
    runtime_inventory: Option<RuntimeInventory>,
    footer: Footer,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MenuUpdate {
    Rebuild,
    UpdateRetainedRows,
}

#[cfg(test)]
const FIXTURE_TARGET_BYTES: u64 = 6_716_356_800;
#[cfg(test)]
const FIXTURE_DRAFT_BYTES: u64 = 253_708_800;
#[cfg(test)]
const FIXTURE_TOTAL_BYTES: u64 = FIXTURE_TARGET_BYTES + FIXTURE_DRAFT_BYTES;
#[cfg(test)]
const FIXTURE_QUANTIZATION: &str = "Q4_K_M";

#[cfg(test)]
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

#[cfg(test)]
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
            Self::Paused => partial_fixture(Download::fixture_paused(
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

        MenuSnapshot::new_fixture(bundle, recommendation, download, runtime, inventory)
            .expect("every built-in fixture is canonical")
    }
}

#[cfg(test)]
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
    Loading,
    Error,
    CleanEligible,
    CleanUnavailable,
    Installed,
    Recovery,
    PausedTransfer,
    #[cfg(test)]
    ActiveTransfer,
    #[cfg(test)]
    FixturePausedTransfer,
    #[cfg(test)]
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
        Self::compose(
            bundle,
            recommendation,
            download,
            runtime,
            runtime_inventory,
            None,
        )
    }

    #[cfg(test)]
    fn new_fixture(
        bundle: Bundle,
        recommendation: Recommendation,
        download: Download,
        runtime: Runtime,
        runtime_inventory: RuntimeInventory,
    ) -> Result<Self, &'static str> {
        Self::compose(
            bundle,
            recommendation,
            download,
            runtime,
            runtime_inventory,
            Some(FIXTURE_QUANTIZATION),
        )
    }

    fn compose(
        bundle: Bundle,
        recommendation: Recommendation,
        download: Download,
        runtime: Runtime,
        runtime_inventory: RuntimeInventory,
        quantization: Option<&'static str>,
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
                sizes: Some((target_bytes, draft_bytes)),
                quantization,
                availability: RecommendationAvailability::Eligible,
            }),
            (Bundle::Absent, Recommendation::Unavailable { reason, sizes }, Download::Idle) => {
                MenuBody::CleanAbsence(RecommendationRow {
                    sizes,
                    quantization,
                    availability: RecommendationAvailability::Unavailable(reason),
                })
            }
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
                quantization,
                active_runtime: matches!(runtime, Runtime::Running),
            }),
            (Bundle::Recovery(reason), Recommendation::Hidden, Download::Idle) => {
                MenuBody::Recovery(RecoveryRow { reason })
            }
            (Bundle::Partial, Recommendation::Hidden, Download::Paused(progress)) => {
                MenuBody::Partial(transfer_row(TransferState::Paused, progress)?)
            }
            #[cfg(test)]
            (Bundle::Partial, Recommendation::Hidden, Download::Active(progress)) => {
                MenuBody::Partial(fixture_transfer_row(TransferState::Active, progress)?)
            }
            #[cfg(test)]
            (Bundle::Partial, Recommendation::Hidden, Download::FixturePaused(progress)) => {
                MenuBody::Partial(fixture_transfer_row(
                    TransferState::FixturePaused,
                    progress,
                )?)
            }
            #[cfg(test)]
            (Bundle::Partial, Recommendation::Hidden, Download::Failed(progress)) => {
                MenuBody::Partial(fixture_transfer_row(TransferState::Failed, progress)?)
            }
            _ => return Err("snapshot combination is not canonical"),
        };

        Ok(Self {
            body,
            runtime: Some(runtime),
            observed_runtime: None,
            bundle_model_id: None,
            runtime_inventory: Some(runtime_inventory),
            footer: Footer {
                inventory: Some(runtime_inventory),
            },
        })
    }

    pub(crate) fn loading() -> Self {
        Self {
            body: MenuBody::Loading,
            runtime: None,
            observed_runtime: None,
            bundle_model_id: None,
            runtime_inventory: None,
            footer: Footer { inventory: None },
        }
    }

    pub(crate) fn error(error: String) -> Self {
        Self {
            body: MenuBody::Error(error),
            runtime: None,
            observed_runtime: None,
            bundle_model_id: None,
            runtime_inventory: None,
            footer: Footer { inventory: None },
        }
    }

    pub(crate) fn is_loading(&self) -> bool {
        matches!(self.body, MenuBody::Loading)
    }

    pub(crate) fn error_message(&self) -> Option<&str> {
        match &self.body {
            MenuBody::Error(error) => Some(error),
            MenuBody::Loading
            | MenuBody::CleanAbsence(_)
            | MenuBody::Verified(_)
            | MenuBody::Recovery(_)
            | MenuBody::Partial(_) => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn section_kinds(&self) -> Vec<MenuSection> {
        match self.body {
            MenuBody::Loading | MenuBody::Error(_) => {
                vec![MenuSection::Header, MenuSection::Footer]
            }
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
            MenuBody::Loading
            | MenuBody::Error(_)
            | MenuBody::Verified(_)
            | MenuBody::Recovery(_)
            | MenuBody::Partial(_) => None,
        }
    }

    pub(crate) fn installed_row(&self) -> Option<&InstalledRow> {
        match &self.body {
            MenuBody::Loading | MenuBody::Error(_) | MenuBody::CleanAbsence(_) => None,
            MenuBody::Verified(row) => Some(row),
            MenuBody::Recovery(_) | MenuBody::Partial(_) => None,
        }
    }

    pub(crate) fn recovery_row(&self) -> Option<&RecoveryRow> {
        match &self.body {
            MenuBody::Recovery(row) => Some(row),
            MenuBody::Loading
            | MenuBody::Error(_)
            | MenuBody::CleanAbsence(_)
            | MenuBody::Verified(_)
            | MenuBody::Partial(_) => None,
        }
    }

    pub(crate) fn transfer_row(&self) -> Option<&TransferRow> {
        match &self.body {
            MenuBody::Partial(row) => Some(row),
            MenuBody::Loading
            | MenuBody::Error(_)
            | MenuBody::CleanAbsence(_)
            | MenuBody::Verified(_)
            | MenuBody::Recovery(_) => None,
        }
    }

    pub(crate) fn footer(&self) -> &Footer {
        &self.footer
    }

    #[cfg(test)]
    pub(crate) fn runtime_label(&self) -> &'static str {
        match &self.body {
            MenuBody::Loading => "Inference: Loading",
            MenuBody::Error(_) => "Inference: Unavailable",
            MenuBody::CleanAbsence(_)
            | MenuBody::Verified(_)
            | MenuBody::Recovery(_)
            | MenuBody::Partial(_) => {
                match self.runtime.expect("observed snapshots have runtime state") {
                    Runtime::Idle => "Inference: Idle",
                    Runtime::Starting => "Inference: Starting",
                    Runtime::Running => "Inference: Running",
                    Runtime::Stopping => "Inference: Stopping",
                    Runtime::Error => "Inference: Error",
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn runtime_inventory_for_test(&self) -> Option<RuntimeInventory> {
        self.runtime_inventory
    }

    #[cfg(test)]
    pub(crate) fn with_running_port(self, port: u16) -> Result<Self, &'static str> {
        self.with_observed_runtime(ObservedRuntimeOwner::Legacy, "fixture-model".into(), port)
    }

    pub(crate) fn with_observed_runtime(
        mut self,
        owner: ObservedRuntimeOwner,
        model_id: String,
        port: u16,
    ) -> Result<Self, &'static str> {
        if self.runtime != Some(Runtime::Running) {
            return Err("runtime identity requires a running inference process");
        }
        self.observed_runtime = Some(ObservedRuntime::new(owner, model_id, port)?);
        Ok(self)
    }

    pub(crate) fn with_bundle_model_id(mut self, model_id: String) -> Result<Self, &'static str> {
        if model_id.is_empty() {
            return Err("bundle identity must not be empty");
        }
        self.bundle_model_id = Some(model_id);
        Ok(self)
    }

    pub(crate) fn observed_runtime(&self) -> Option<&ObservedRuntime> {
        self.observed_runtime
            .as_ref()
            .filter(|_| matches!(self.runtime, Some(Runtime::Running)))
    }

    pub(crate) fn busy_runtime_model_matching_bundle(&self) -> Option<&str> {
        if !self.recovery_row()?.is_busy() {
            return None;
        }
        let runtime = self.observed_runtime()?;
        let bundle_model_id = self.bundle_model_id.as_deref()?;
        (runtime.model_id() == bundle_model_id).then_some(bundle_model_id)
    }

    #[cfg(test)]
    pub(crate) fn runtime_api_label(&self) -> Option<String> {
        self.observed_runtime()
            .map(|runtime| format!("API · 127.0.0.1:{}", runtime.port()))
    }

    #[cfg(test)]
    pub(crate) fn runtime_curl_command(&self) -> Option<String> {
        self.observed_runtime()
            .map(|runtime| format!("curl http://127.0.0.1:{}/v1/models", runtime.port()))
    }

    pub(crate) fn update_from(&self, previous: Option<&Self>) -> MenuUpdate {
        if previous.is_some_and(|previous| {
            previous.composition() == self.composition()
                && previous.busy_runtime_model_matching_bundle().is_some()
                    == self.busy_runtime_model_matching_bundle().is_some()
        }) {
            MenuUpdate::UpdateRetainedRows
        } else {
            MenuUpdate::Rebuild
        }
    }

    #[cfg(test)]
    pub(crate) fn apply_fixture_action(&self, action: MenuAction) -> Option<Self> {
        let download = match (&self.body, action) {
            (MenuBody::CleanAbsence(row), MenuAction::Start)
                if matches!(row.availability, RecommendationAvailability::Eligible) =>
            {
                Download::active(
                    0,
                    row.sizes
                        .expect("eligible fixtures include exact recommendation bytes")
                        .0
                        .saturating_add(
                            row.sizes
                                .expect("eligible fixtures include exact recommendation bytes")
                                .1,
                        ),
                    DownloadPhase::Preparing,
                )
            }
            (MenuBody::Partial(row), MenuAction::Pause) if row.state == TransferState::Active => {
                Download::fixture_paused(
                    row.progress.completed_bytes,
                    row.progress.total_bytes,
                    row.phase.expect("fixtures always have a phase"),
                )
            }
            (MenuBody::Partial(row), MenuAction::Resume)
                if row.state == TransferState::FixturePaused =>
            {
                Download::active(
                    row.progress.completed_bytes,
                    row.progress.total_bytes,
                    row.phase.expect("fixtures always have a phase"),
                )
            }
            (MenuBody::Partial(row), MenuAction::Retry) if row.state == TransferState::Failed => {
                Download::active(
                    row.progress.completed_bytes,
                    row.progress.total_bytes,
                    row.phase.expect("fixtures always have a phase"),
                )
            }
            _ => return None,
        };

        Self::new(
            Bundle::Partial,
            Recommendation::Hidden,
            download,
            self.runtime.expect("fixtures always have runtime state"),
            self.footer
                .inventory
                .expect("fixtures always have runtime inventory"),
        )
        .ok()
    }
    fn composition(&self) -> MenuComposition {
        match &self.body {
            MenuBody::Loading => MenuComposition::Loading,
            MenuBody::Error(_) => MenuComposition::Error,
            MenuBody::CleanAbsence(row) => match row.availability {
                RecommendationAvailability::Eligible => MenuComposition::CleanEligible,
                RecommendationAvailability::Unavailable(_) => MenuComposition::CleanUnavailable,
            },
            MenuBody::Verified(_) => MenuComposition::Installed,
            MenuBody::Recovery(_) => MenuComposition::Recovery,
            MenuBody::Partial(row) => match row.state {
                TransferState::Paused => MenuComposition::PausedTransfer,
                #[cfg(test)]
                TransferState::Active => MenuComposition::ActiveTransfer,
                #[cfg(test)]
                TransferState::FixturePaused => MenuComposition::FixturePausedTransfer,
                #[cfg(test)]
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
    Ok(TransferRow {
        state,
        progress,
        #[cfg(test)]
        phase: None,
    })
}

#[cfg(test)]
fn fixture_transfer_row(
    state: TransferState,
    progress: FixtureTransferProgress,
) -> Result<TransferRow, &'static str> {
    let mut row = transfer_row(state, progress.progress)?;
    row.phase = Some(progress.phase);
    Ok(row)
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

fn quantization_segment(quantization: Option<&str>) -> String {
    quantization.map_or_else(String::new, |value| format!("{value} · "))
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
