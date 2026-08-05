use std::cell::{Cell, RefCell};

use objc2::rc::Retained;
use objc2::runtime::{AnyObject, Sel};
use objc2::{define_class, msg_send, AnyThread, DefinedClass, MainThreadMarker, MainThreadOnly};
use objc2_app_kit::{
    NSAccessibility, NSBezierPath, NSBox, NSBoxType, NSButton, NSColor, NSControlSize, NSEvent,
    NSFont, NSImage, NSImageView, NSLayoutAttribute, NSProgressIndicator, NSProgressIndicatorStyle,
    NSStackView, NSTextAlignment, NSTextField, NSTrackingArea, NSTrackingAreaOptions,
    NSUserInterfaceLayoutOrientation, NSView,
};
use objc2_foundation::{NSInteger, NSPoint, NSRect, NSSize, NSString};

#[cfg(any(test, debug_assertions))]
use crate::menu::presentation::{InlineCancelState, MenuAction};
use crate::menu::presentation::{MenuLayout, MenuSnapshot, RecommendationRow, TransferRow};

const ROW_WIDTH: f64 = MenuLayout::BASE_WIDTH;
const CONTENT_INSET: f64 = MenuLayout::OUTER_PADDING + MenuLayout::INNER_PADDING;
const SECTION_HEIGHT: f64 = 28.0;
const HEADER_HEIGHT: f64 = 52.0;
const FOOTER_HEIGHT: f64 = 28.0;
const SEPARATOR_HEIGHT: f64 = 9.0;
const ICON_IMAGE_SIZE: f64 = 16.0;
const FINAL_CONTENT_SPACER_HEIGHT: f64 = 4.0;

#[derive(Clone, Copy)]
pub(super) struct Actions {
    #[cfg(any(test, debug_assertions))]
    pub(super) start: Sel,
    #[cfg(any(test, debug_assertions))]
    pub(super) pause: Sel,
    #[cfg(any(test, debug_assertions))]
    pub(super) resume: Sel,
    #[cfg(any(test, debug_assertions))]
    pub(super) retry: Sel,
    #[cfg(any(test, debug_assertions))]
    pub(super) cancel: Sel,
    #[cfg(any(test, debug_assertions))]
    pub(super) keep_partial: Sel,
    #[cfg(any(test, debug_assertions))]
    pub(super) discard_partial: Sel,
    pub(super) quit: Sel,
}

pub(super) struct MenuRows {
    header: HeaderRow,
    body: BodyRows,
    footer: FooterRow,
}

pub(super) struct PopoverContent {
    pub(super) view: Retained<NSView>,
    pub(super) rows: MenuRows,
    #[cfg(test)]
    #[allow(dead_code)]
    pub(super) quit_button: Retained<NSButton>,
    #[cfg(test)]
    #[allow(dead_code)]
    pub(super) action_buttons: Vec<Retained<NSButton>>,
}

impl MenuRows {
    pub(super) fn build(
        snapshot: &MenuSnapshot,
        target: Option<&AnyObject>,
        actions: Actions,
        mtm: MainThreadMarker,
    ) -> PopoverContent {
        let mut layout = ContentLayout::new(content_height(snapshot), mtm);
        let header = HeaderRow::build(snapshot, mtm);
        layout.add(&header.root, HEADER_HEIGHT);
        layout.add_separator(mtm);

        let body = if snapshot.is_loading() {
            layout.add(&section_header("Status", mtm), SECTION_HEIGHT);
            let row = StatusNativeRow::build("Loading Loxa status…", mtm);
            layout.add(&row.root, 56.0);
            BodyRows::Status(row)
        } else if let Some(error) = snapshot.error_message() {
            layout.add(&section_header("Status", mtm), SECTION_HEIGHT);
            let row = StatusNativeRow::build(error, mtm);
            layout.add(&row.root, 56.0);
            BodyRows::Status(row)
        } else if let Some(recommendation) = snapshot.recommendation_row() {
            layout.add(&section_header("Installed", mtm), SECTION_HEIGHT);
            layout.add(&empty_installed_row(mtm), MenuLayout::model_row_height());
            layout.add_separator(mtm);
            layout.add(
                &section_header("Recommended for this Mac", mtm),
                SECTION_HEIGHT,
            );
            let row = RecommendationNativeRow::build(recommendation, target, actions, mtm);
            layout.add(&row.root, 56.0);
            BodyRows::Recommendation(row)
        } else if let Some(installed) = snapshot.installed_row() {
            layout.add(&section_header("Installed", mtm), SECTION_HEIGHT);
            let row = InstalledNativeRow::build(installed, mtm);
            layout.add(&row.root, 56.0);
            BodyRows::Installed(row)
        } else if let Some(recovery) = snapshot.recovery_row() {
            layout.add(&section_header("Installed", mtm), SECTION_HEIGHT);
            let row = RecoveryNativeRow::build(recovery.detail(), mtm);
            layout.add(&row.root, 56.0);
            BodyRows::Recovery(row)
        } else if let Some(transfer) = snapshot.transfer_row() {
            layout.add(&section_header("Downloading", mtm), SECTION_HEIGHT);
            let row = TransferNativeRow::build(transfer, target, actions, mtm);
            layout.add(&row.root, MenuLayout::transfer_row_height());
            BodyRows::Transfer(row)
        } else {
            unreachable!("every menu snapshot has exactly one body row")
        };

        #[cfg(test)]
        let action_buttons = body.action_buttons();

        layout.add_separator(mtm);
        let footer = FooterRow::build(snapshot, mtm);
        layout.add(&footer.root, FOOTER_HEIGHT);
        layout.add_separator(mtm);
        let quit = QuitRow::build(target, actions.quit, mtm);
        #[cfg(test)]
        let quit_button = quit.button.clone();
        layout.add(&quit.root, FOOTER_HEIGHT);
        layout.add_spacer(FINAL_CONTENT_SPACER_HEIGHT);

        PopoverContent {
            view: layout.finish(),
            rows: Self {
                header,
                body,
                footer,
            },
            #[cfg(test)]
            quit_button,
            #[cfg(test)]
            action_buttons,
        }
    }

    pub(super) fn update(
        &mut self,
        snapshot: &MenuSnapshot,
        #[cfg(any(test, debug_assertions))] cancel: &InlineCancelState,
    ) {
        self.header.update(snapshot);
        self.footer.update(snapshot);
        match &mut self.body {
            BodyRows::Status(row) => {
                if snapshot.is_loading() {
                    row.update("Loading Loxa status…");
                } else if let Some(error) = snapshot.error_message() {
                    row.update(error);
                }
            }
            BodyRows::Recommendation(row) => {
                if let Some(recommendation) = snapshot.recommendation_row() {
                    row.update(recommendation);
                }
            }
            BodyRows::Installed(row) => {
                if let Some(installed) = snapshot.installed_row() {
                    row.update(installed);
                }
            }
            BodyRows::Recovery(row) => {
                if let Some(recovery) = snapshot.recovery_row() {
                    row.update(recovery.detail());
                }
            }
            BodyRows::Transfer(row) => {
                if let Some(transfer) = snapshot.transfer_row() {
                    row.update(transfer);
                    #[cfg(any(test, debug_assertions))]
                    row.update_cancel_controls(cancel);
                }
            }
        }
    }

    #[cfg(any(test, debug_assertions))]
    pub(super) fn update_cancel_controls(&mut self, cancel: &InlineCancelState) {
        if let BodyRows::Transfer(row) = &mut self.body {
            row.update_cancel_controls(cancel);
        }
    }

    #[cfg(any(test, debug_assertions))]
    pub(super) fn show_cancel_confirmation(&mut self, cancel: &mut InlineCancelState) {
        cancel.activate_cancel();
        self.update_cancel_controls(cancel);
    }
}

struct ContentLayout {
    root: Retained<NSView>,
    next_y: f64,
}

impl ContentLayout {
    fn new(height: f64, mtm: MainThreadMarker) -> Self {
        Self {
            root: NSView::initWithFrame(NSView::alloc(mtm), rect(0.0, 0.0, ROW_WIDTH, height)),
            next_y: height,
        }
    }

    fn add(&mut self, view: &NSView, height: f64) {
        self.next_y -= height;
        view.setFrame(rect(0.0, self.next_y, ROW_WIDTH, height));
        self.root.addSubview(view);
    }

    fn add_separator(&mut self, mtm: MainThreadMarker) {
        let separator = NSBox::initWithFrame(
            NSBox::alloc(mtm),
            rect(0.0, 0.0, ROW_WIDTH, SEPARATOR_HEIGHT),
        );
        separator.setBoxType(NSBoxType::Separator);
        self.add(&separator, SEPARATOR_HEIGHT);
    }

    fn add_spacer(&mut self, height: f64) {
        self.next_y -= height;
    }

    fn finish(self) -> Retained<NSView> {
        debug_assert_eq!(
            self.next_y, 0.0,
            "popover content height must match its rows"
        );
        self.root
    }
}

fn content_height(snapshot: &MenuSnapshot) -> f64 {
    let common =
        HEADER_HEIGHT + 2.0 * FOOTER_HEIGHT + 3.0 * SEPARATOR_HEIGHT + FINAL_CONTENT_SPACER_HEIGHT;
    if snapshot.recommendation_row().is_some() {
        common
            + SECTION_HEIGHT
            + MenuLayout::model_row_height()
            + SEPARATOR_HEIGHT
            + SECTION_HEIGHT
            + 56.0
    } else if snapshot.transfer_row().is_some() {
        common + SECTION_HEIGHT + MenuLayout::transfer_row_height()
    } else {
        common + SECTION_HEIGHT + 56.0
    }
}

enum BodyRows {
    Status(StatusNativeRow),
    Recommendation(RecommendationNativeRow),
    Installed(InstalledNativeRow),
    Recovery(RecoveryNativeRow),
    Transfer(TransferNativeRow),
}

#[cfg(test)]
impl BodyRows {
    fn action_buttons(&self) -> Vec<Retained<NSButton>> {
        match self {
            Self::Status(_) => Vec::new(),
            Self::Recommendation(row) => row.action_buttons(),
            Self::Transfer(row) => row.action_buttons(),
            Self::Installed(_) | Self::Recovery(_) => Vec::new(),
        }
    }
}

struct HeaderRow {
    root: Retained<NSView>,
    runtime_label: Retained<NSTextField>,
}

impl HeaderRow {
    fn build(snapshot: &MenuSnapshot, mtm: MainThreadMarker) -> Self {
        let root = row_shell(HEADER_HEIGHT, mtm);
        let stack = vertical_stack(mtm);
        let title = primary_label("Loxa", mtm);
        let runtime_label = secondary_label(snapshot.runtime_label(), mtm);

        stack.addArrangedSubview(&title);
        stack.addArrangedSubview(&runtime_label);
        pin_to_content(&root, &stack);

        Self {
            root,
            runtime_label,
        }
    }

    fn update(&mut self, snapshot: &MenuSnapshot) {
        set_label(&self.runtime_label, snapshot.runtime_label());
    }
}

struct InstalledNativeRow {
    root: Retained<NSView>,
    subtitle: Retained<NSTextField>,
    runtime_note: Retained<NSTextField>,
}

impl InstalledNativeRow {
    fn build(installed: &crate::menu::presentation::InstalledRow, mtm: MainThreadMarker) -> Self {
        let root = row_shell(56.0, mtm);
        let stack = horizontal_stack(mtm);
        let icon = icon_container("sparkles", None, mtm);
        let labels = vertical_stack(mtm);
        let title_text = format!("Gemma 4 12B · {}", installed.verification_label());
        let title = primary_label(&title_text, mtm);
        let subtitle = secondary_label(&installed.subtitle(), mtm);
        set_label_with_detail(&subtitle, &installed.subtitle(), &installed.size_detail());
        let runtime_note = secondary_label(installed.runtime_note().unwrap_or(""), mtm);
        runtime_note.setHidden(installed.runtime_note().is_none());

        labels.addArrangedSubview(&title);
        labels.addArrangedSubview(&subtitle);
        labels.addArrangedSubview(&runtime_note);
        stack.addArrangedSubview(&icon);
        stack.addArrangedSubview(&labels);
        pin_to_content(&root, &stack);

        Self {
            root,
            subtitle,
            runtime_note,
        }
    }

    fn update(&mut self, installed: &crate::menu::presentation::InstalledRow) {
        set_label_with_detail(
            &self.subtitle,
            &installed.subtitle(),
            &installed.size_detail(),
        );
        match installed.runtime_note() {
            Some(note) => {
                set_label(&self.runtime_note, note);
                self.runtime_note.setHidden(false);
            }
            None => self.runtime_note.setHidden(true),
        }
    }
}

struct RecoveryNativeRow {
    root: Retained<NSView>,
    detail: Retained<NSTextField>,
}

impl RecoveryNativeRow {
    fn build(detail: &str, mtm: MainThreadMarker) -> Self {
        let root = row_shell(56.0, mtm);
        let stack = horizontal_stack(mtm);
        let icon = icon_container(
            "exclamationmark.triangle",
            Some("Bundle recovery required"),
            mtm,
        );
        let labels = vertical_stack(mtm);
        labels.addArrangedSubview(&primary_label("Managed bundle needs recovery", mtm));
        let detail_label = secondary_label(detail, mtm);
        labels.addArrangedSubview(&detail_label);
        stack.addArrangedSubview(&icon);
        stack.addArrangedSubview(&labels);
        pin_to_content(&root, &stack);
        Self {
            root,
            detail: detail_label,
        }
    }

    fn update(&mut self, detail: &str) {
        set_label(&self.detail, detail);
    }
}

struct StatusNativeRow {
    root: Retained<NSView>,
    detail: Retained<NSTextField>,
}

impl StatusNativeRow {
    fn build(detail: &str, mtm: MainThreadMarker) -> Self {
        let root = row_shell(56.0, mtm);
        let labels = vertical_stack(mtm);
        labels.addArrangedSubview(&primary_label("Loxa status", mtm));
        let detail_label = secondary_label(detail, mtm);
        labels.addArrangedSubview(&detail_label);
        pin_to_content(&root, &labels);
        Self {
            root,
            detail: detail_label,
        }
    }

    fn update(&mut self, detail: &str) {
        set_label(&self.detail, detail);
    }
}

struct RecommendationNativeRow {
    root: Retained<NSView>,
    subtitle: Retained<NSTextField>,
    availability: Retained<NSTextField>,
    #[cfg(test)]
    action_button: Option<Retained<NSButton>>,
}

impl RecommendationNativeRow {
    fn build(
        recommendation: &RecommendationRow,
        target: Option<&AnyObject>,
        actions: Actions,
        mtm: MainThreadMarker,
    ) -> Self {
        #[cfg(any(test, debug_assertions))]
        let actionable = recommendation.action().is_some();
        #[cfg(not(any(test, debug_assertions)))]
        let actionable = false;
        let root = if actionable {
            hover_row_shell(56.0, mtm).into_super()
        } else {
            row_shell(56.0, mtm)
        };
        let stack = horizontal_stack(mtm);
        let icon = icon_container("sparkles", None, mtm);
        let labels = vertical_stack(mtm);
        labels.addArrangedSubview(&primary_label("Gemma 4", mtm));
        let subtitle_text = recommendation.subtitle();
        let subtitle = secondary_label(subtitle_text.as_deref().unwrap_or(""), mtm);
        apply_recommendation_size(&subtitle, recommendation);
        labels.addArrangedSubview(&subtitle);
        let availability = secondary_label(
            recommendation
                .disabled_reason()
                .unwrap_or("Ready to download"),
            mtm,
        );
        labels.addArrangedSubview(&availability);
        stack.addArrangedSubview(&icon);
        stack.addArrangedSubview(&labels);

        #[cfg(test)]
        let mut action_button = None;
        #[cfg(any(test, debug_assertions))]
        if actionable {
            let button = icon_button(
                "arrow.down.circle",
                "Download Gemma 4 12B",
                target,
                actions.start,
                mtm,
            );
            #[cfg(test)]
            {
                action_button = Some(button.clone());
            }
            stack.addArrangedSubview(&button);
        }
        #[cfg(not(any(test, debug_assertions)))]
        let _ = (target, actions);
        pin_to_content(&root, &stack);

        Self {
            root,
            subtitle,
            availability,
            #[cfg(test)]
            action_button,
        }
    }

    fn update(&mut self, recommendation: &RecommendationRow) {
        apply_recommendation_size(&self.subtitle, recommendation);
        set_label(
            &self.availability,
            recommendation
                .disabled_reason()
                .unwrap_or("Ready to download"),
        );
    }

    #[cfg(test)]
    fn action_buttons(&self) -> Vec<Retained<NSButton>> {
        self.action_button.iter().cloned().collect()
    }
}

struct TransferNativeRow {
    root: Retained<NSView>,
    phase: Retained<NSTextField>,
    progress_text: Retained<NSTextField>,
    progress: Retained<NSProgressIndicator>,
    #[cfg(test)]
    primary_button: Option<Retained<NSButton>>,
    #[cfg(any(test, debug_assertions))]
    cancel_button: Retained<NSButton>,
    #[cfg(any(test, debug_assertions))]
    keep_button: Retained<NSButton>,
    #[cfg(any(test, debug_assertions))]
    discard_button: Retained<NSButton>,
}

impl TransferNativeRow {
    fn build(
        transfer: &TransferRow,
        target: Option<&AnyObject>,
        actions: Actions,
        mtm: MainThreadMarker,
    ) -> Self {
        #[cfg(any(test, debug_assertions))]
        let root = if transfer.has_fixture_action() {
            hover_row_shell(MenuLayout::transfer_row_height(), mtm).into_super()
        } else {
            row_shell(MenuLayout::transfer_row_height(), mtm)
        };
        #[cfg(not(any(test, debug_assertions)))]
        let root = row_shell(MenuLayout::transfer_row_height(), mtm);
        let stack = vertical_stack(mtm);
        let title_row = horizontal_stack(mtm);
        title_row.addArrangedSubview(&primary_label("Gemma 4 12B", mtm));
        #[cfg(test)]
        let mut primary_button = None;
        #[cfg(any(test, debug_assertions))]
        if transfer.has_fixture_action() {
            let primary = primary_action_button(transfer.primary_action(), target, actions, mtm);
            #[cfg(test)]
            {
                primary_button = Some(primary.clone());
            }
            title_row.addArrangedSubview(&primary);
        }
        stack.addArrangedSubview(&title_row);

        let phase = secondary_label(transfer.phase_label(), mtm);
        let progress_text = secondary_label(&transfer.progress_text(), mtm);
        set_label_with_detail(
            &progress_text,
            &transfer.progress_text(),
            &transfer.progress_detail(),
        );
        let progress = NSProgressIndicator::initWithFrame(
            NSProgressIndicator::alloc(mtm),
            rect(0.0, 0.0, 1.0, 8.0),
        );
        progress.setIndeterminate(false);
        progress.setMinValue(0.0);
        progress.setMaxValue(1.0);
        progress.setDoubleValue(transfer.progress_fraction());
        progress.setStyle(NSProgressIndicatorStyle::Bar);
        progress.setTranslatesAutoresizingMaskIntoConstraints(false);
        activate(progress.heightAnchor().constraintEqualToConstant(8.0));
        stack.addArrangedSubview(&phase);
        stack.addArrangedSubview(&progress);
        stack.addArrangedSubview(&progress_text);

        #[cfg(any(test, debug_assertions))]
        let action_row = horizontal_stack(mtm);
        #[cfg(any(test, debug_assertions))]
        let cancel_button = text_button(
            MenuAction::Cancel
                .confirmation_label()
                .expect("cancel is a confirmation action"),
            MenuAction::Cancel
                .accessibility_label()
                .expect("cancel has an accessibility label"),
            target,
            actions.cancel,
            mtm,
        );
        #[cfg(any(test, debug_assertions))]
        let keep_button = text_button(
            MenuAction::KeepPartial
                .confirmation_label()
                .expect("keep-partial is a confirmation action"),
            MenuAction::KeepPartial
                .accessibility_label()
                .expect("keep-partial has an accessibility label"),
            target,
            actions.keep_partial,
            mtm,
        );
        #[cfg(any(test, debug_assertions))]
        let discard_button = text_button(
            MenuAction::DiscardPartial
                .confirmation_label()
                .expect("discard-partial is a confirmation action"),
            MenuAction::DiscardPartial
                .accessibility_label()
                .expect("discard-partial has an accessibility label"),
            target,
            actions.discard_partial,
            mtm,
        );
        #[cfg(any(test, debug_assertions))]
        if transfer.has_fixture_action() {
            keep_button.setHidden(true);
            discard_button.setHidden(true);
            action_row.addArrangedSubview(&cancel_button);
            action_row.addArrangedSubview(&keep_button);
            action_row.addArrangedSubview(&discard_button);
            stack.addArrangedSubview(&action_row);
        }
        #[cfg(not(any(test, debug_assertions)))]
        let _ = (target, actions);
        pin_to_content(&root, &stack);

        Self {
            root,
            phase,
            progress_text,
            progress,
            #[cfg(test)]
            primary_button,
            #[cfg(any(test, debug_assertions))]
            cancel_button,
            #[cfg(any(test, debug_assertions))]
            keep_button,
            #[cfg(any(test, debug_assertions))]
            discard_button,
        }
    }

    fn update(&mut self, transfer: &TransferRow) {
        set_label(&self.phase, transfer.phase_label());
        set_label_with_detail(
            &self.progress_text,
            &transfer.progress_text(),
            &transfer.progress_detail(),
        );
        self.progress.setDoubleValue(transfer.progress_fraction());
    }

    #[cfg(any(test, debug_assertions))]
    fn update_cancel_controls(&mut self, cancel: &InlineCancelState) {
        let confirming = cancel
            .visible_actions()
            .contains(&MenuAction::DiscardPartial);
        self.cancel_button.setHidden(confirming);
        self.keep_button.setHidden(!confirming);
        self.discard_button.setHidden(!confirming);
    }

    #[cfg(test)]
    fn action_buttons(&self) -> Vec<Retained<NSButton>> {
        vec![
            self.primary_button
                .clone()
                .expect("fixture transfer rows retain their primary action"),
            self.cancel_button.clone(),
            self.keep_button.clone(),
            self.discard_button.clone(),
        ]
    }
}

struct FooterRow {
    root: Retained<NSView>,
    text: Retained<NSTextField>,
}

impl FooterRow {
    fn build(snapshot: &MenuSnapshot, mtm: MainThreadMarker) -> Self {
        let root = row_shell(FOOTER_HEIGHT, mtm);
        let text = secondary_label(&footer_text(snapshot), mtm);
        pin_to_content(&root, &text);
        Self { root, text }
    }

    fn update(&mut self, snapshot: &MenuSnapshot) {
        set_label(&self.text, &footer_text(snapshot));
    }
}

struct QuitRow {
    root: Retained<NSView>,
    #[allow(dead_code)]
    button: Retained<NSButton>,
}

impl QuitRow {
    fn build(target: Option<&AnyObject>, quit: Sel, mtm: MainThreadMarker) -> Self {
        let root = row_shell(FOOTER_HEIGHT, mtm);
        let button = text_button("Quit Loxa", "Quit Loxa", target, quit, mtm);
        button.setKeyEquivalent(&NSString::from_str("q"));
        button.setKeyEquivalentModifierMask(objc2_app_kit::NSEventModifierFlags::Command);
        pin_to_content(&root, &button);
        Self { root, button }
    }
}

fn section_header(title: &str, mtm: MainThreadMarker) -> Retained<NSView> {
    let root = row_shell(SECTION_HEIGHT, mtm);
    let label = primary_label(title, mtm);
    pin_to_content(&root, &label);
    root
}

fn empty_installed_row(mtm: MainThreadMarker) -> Retained<NSView> {
    let root = row_shell(MenuLayout::model_row_height(), mtm);
    let outline = DashedOutlineView::new(
        rect(0.0, 0.0, ROW_WIDTH, MenuLayout::model_row_height()),
        mtm,
    );
    pin_to_content(&root, &outline);

    let label = secondary_label("No models yet", mtm);
    label.setAlignment(NSTextAlignment::Center);
    label.setTranslatesAutoresizingMaskIntoConstraints(false);
    root.addSubview(&label);
    activate(
        label
            .centerXAnchor()
            .constraintEqualToAnchor(&root.centerXAnchor()),
    );
    activate(
        label
            .centerYAnchor()
            .constraintEqualToAnchor(&root.centerYAnchor()),
    );
    root
}

struct HoverRowIvars {
    hovered: Cell<bool>,
    tracking_area: RefCell<Option<Retained<NSTrackingArea>>>,
}

define_class!(
    // SAFETY: NSView has no subclassing requirements, and this class has no Drop implementation.
    #[unsafe(super(NSView))]
    #[name = "LoxaHoverRowView"]
    #[thread_kind = MainThreadOnly]
    #[ivars = HoverRowIvars]
    struct HoverRowView;

    impl HoverRowView {
        #[unsafe(method(drawRect:))]
        fn draw(&self, _dirty_rect: NSRect) {
            if !self.draws_hover_background() {
                return;
            }

            NSColor::labelColor()
                .colorWithAlphaComponent(0.08)
                .setFill();
            NSBezierPath::bezierPathWithRoundedRect_xRadius_yRadius(
                self.hover_bounds(),
                self.hover_radius(),
                self.hover_radius(),
            )
            .fill();
        }

        #[unsafe(method(updateTrackingAreas))]
        fn update_tracking_areas(&self) {
            if let Some(tracking_area) = self.ivars().tracking_area.borrow_mut().take() {
                self.removeTrackingArea(&tracking_area);
            }

            // SAFETY: NSView implements updateTrackingAreas with this exact ABI.
            unsafe { msg_send![super(self), updateTrackingAreas] }

            let tracking_area = unsafe {
                NSTrackingArea::initWithRect_options_owner_userInfo(
                    NSTrackingArea::alloc(),
                    self.bounds(),
                    NSTrackingAreaOptions::MouseEnteredAndExited
                        | NSTrackingAreaOptions::ActiveInActiveApp
                        | NSTrackingAreaOptions::InVisibleRect,
                    Some(self),
                    None,
                )
            };
            self.addTrackingArea(&tracking_area);
            *self.ivars().tracking_area.borrow_mut() = Some(tracking_area);
        }

        #[unsafe(method(mouseEntered:))]
        fn mouse_entered(&self, _event: &NSEvent) {
            self.ivars().hovered.set(true);
            self.setNeedsDisplay(true);
        }

        #[unsafe(method(mouseExited:))]
        fn mouse_exited(&self, _event: &NSEvent) {
            self.ivars().hovered.set(false);
            self.setNeedsDisplay(true);
        }
    }
);

impl HoverRowView {
    fn new(frame: NSRect, mtm: MainThreadMarker) -> Retained<Self> {
        let this = Self::alloc(mtm).set_ivars(HoverRowIvars {
            hovered: Cell::new(false),
            tracking_area: RefCell::new(None),
        });
        // SAFETY: NSView's initWithFrame: selector has the expected signature.
        unsafe { msg_send![super(this), initWithFrame: frame] }
    }

    fn draws_hover_background(&self) -> bool {
        self.ivars().hovered.get()
    }

    fn hover_radius(&self) -> f64 {
        MenuLayout::HOVER_RADIUS
    }

    fn hover_bounds(&self) -> NSRect {
        let mut bounds = self.bounds();
        bounds.origin.x += MenuLayout::OUTER_PADDING;
        bounds.size.width -= 2.0 * MenuLayout::OUTER_PADDING;
        bounds
    }
}

define_class!(
    // SAFETY: NSView has no subclassing requirements, and this class has no Drop implementation.
    #[unsafe(super(NSView))]
    #[name = "LoxaDashedOutlineView"]
    #[thread_kind = MainThreadOnly]
    #[ivars = ()]
    struct DashedOutlineView;

    impl DashedOutlineView {
        #[unsafe(method(drawRect:))]
        fn draw(&self, _dirty_rect: NSRect) {
            let mut bounds = self.bounds();
            bounds.origin.x += 0.5;
            bounds.origin.y += 0.5;
            bounds.size.width -= 1.0;
            bounds.size.height -= 1.0;

            let outline = NSBezierPath::bezierPathWithRoundedRect_xRadius_yRadius(
                bounds,
                MenuLayout::HOVER_RADIUS,
                MenuLayout::HOVER_RADIUS,
            );
            outline.setLineWidth(1.0);
            let dashes = [3.0, 3.0];
            // SAFETY: dashes points to two live CGFloat values for this immediate AppKit call.
            unsafe {
                outline.setLineDash_count_phase(
                    dashes.as_ptr(),
                    dashes.len() as NSInteger,
                    0.0,
                );
            }
            NSColor::separatorColor().setStroke();
            outline.stroke();
        }
    }
);

impl DashedOutlineView {
    fn new(frame: NSRect, mtm: MainThreadMarker) -> Retained<Self> {
        let this = Self::alloc(mtm).set_ivars(());
        // SAFETY: NSView's initWithFrame: selector has the expected signature.
        unsafe { msg_send![super(this), initWithFrame: frame] }
    }
}

fn row_shell(height: f64, mtm: MainThreadMarker) -> Retained<NSView> {
    let width = MenuLayout::width_for(ROW_WIDTH);
    debug_assert_eq!(
        MenuLayout::content_width(width),
        width - 2.0 * CONTENT_INSET,
        "the 300 pt shell and native content inset must stay in lockstep"
    );
    NSView::initWithFrame(NSView::alloc(mtm), rect(0.0, 0.0, width, height))
}

fn hover_row_shell(height: f64, mtm: MainThreadMarker) -> Retained<HoverRowView> {
    let width = MenuLayout::width_for(ROW_WIDTH);
    debug_assert_eq!(
        MenuLayout::content_width(width),
        width - 2.0 * CONTENT_INSET,
        "the 300 pt shell and native content inset must stay in lockstep"
    );
    HoverRowView::new(rect(0.0, 0.0, width, height), mtm)
}

fn vertical_stack(mtm: MainThreadMarker) -> Retained<NSStackView> {
    let stack = NSStackView::new(mtm);
    stack.setOrientation(NSUserInterfaceLayoutOrientation::Vertical);
    stack.setAlignment(NSLayoutAttribute::Leading);
    stack.setSpacing(2.0);
    stack
}

fn horizontal_stack(mtm: MainThreadMarker) -> Retained<NSStackView> {
    let stack = NSStackView::new(mtm);
    stack.setOrientation(NSUserInterfaceLayoutOrientation::Horizontal);
    stack.setAlignment(NSLayoutAttribute::CenterY);
    stack.setSpacing(6.0);
    stack
}

fn primary_label(text: &str, mtm: MainThreadMarker) -> Retained<NSTextField> {
    label(text, MenuLayout::primary_font_size(), true, mtm)
}

fn secondary_label(text: &str, mtm: MainThreadMarker) -> Retained<NSTextField> {
    label(text, MenuLayout::secondary_font_size(), false, mtm)
}

fn label(text: &str, size: f64, primary: bool, mtm: MainThreadMarker) -> Retained<NSTextField> {
    let label = NSTextField::labelWithString(&NSString::from_str(text), mtm);
    label.setFont(Some(&NSFont::systemFontOfSize(size)));
    let color = if primary {
        NSColor::labelColor()
    } else {
        NSColor::secondaryLabelColor()
    };
    label.setTextColor(Some(&color));
    label.setMaximumNumberOfLines(1);
    label
}

fn icon_container(
    symbol: &str,
    description: Option<&str>,
    mtm: MainThreadMarker,
) -> Retained<NSBox> {
    let container = icon_container_shell(mtm);
    let image = NSImageView::new(mtm);
    let description = description.map(NSString::from_str);
    if let Some(symbol) = NSImage::imageWithSystemSymbolName_accessibilityDescription(
        &NSString::from_str(symbol),
        description.as_deref(),
    ) {
        image.setImage(Some(&symbol));
    }
    image.setContentTintColor(Some(&NSColor::secondaryLabelColor()));
    if description.is_none() {
        image.setAccessibilityElement(false);
    }
    add_centered_icon_image(&container, &image);
    container
}

fn icon_container_shell(mtm: MainThreadMarker) -> Retained<NSBox> {
    let container = NSBox::initWithFrame(
        NSBox::alloc(mtm),
        rect(
            0.0,
            0.0,
            MenuLayout::icon_container(),
            MenuLayout::icon_container(),
        ),
    );
    container.setBoxType(NSBoxType::Custom);
    container.setBorderWidth(0.0);
    container.setCornerRadius(MenuLayout::icon_container() / 2.0);
    container.setFillColor(&NSColor::controlBackgroundColor());
    container.setTranslatesAutoresizingMaskIntoConstraints(false);
    activate(
        container
            .widthAnchor()
            .constraintEqualToConstant(MenuLayout::icon_container()),
    );
    activate(
        container
            .heightAnchor()
            .constraintEqualToConstant(MenuLayout::icon_container()),
    );
    container
}

fn add_centered_icon_image(container: &NSBox, image: &NSImageView) {
    image.setTranslatesAutoresizingMaskIntoConstraints(false);
    container.addSubview(image);
    activate(
        image
            .widthAnchor()
            .constraintEqualToConstant(ICON_IMAGE_SIZE),
    );
    activate(
        image
            .heightAnchor()
            .constraintEqualToConstant(ICON_IMAGE_SIZE),
    );
    activate(
        image
            .centerXAnchor()
            .constraintEqualToAnchor(&container.centerXAnchor()),
    );
    activate(
        image
            .centerYAnchor()
            .constraintEqualToAnchor(&container.centerYAnchor()),
    );
}

#[cfg(any(test, debug_assertions))]
fn primary_action_button(
    action: MenuAction,
    target: Option<&AnyObject>,
    selectors: Actions,
    mtm: MainThreadMarker,
) -> Retained<NSButton> {
    let (symbol, label, selector) = match action {
        MenuAction::Pause => ("pause.fill", "Pause download", selectors.pause),
        MenuAction::Resume => ("play.fill", "Resume download", selectors.resume),
        MenuAction::Retry => ("arrow.clockwise", "Retry download", selectors.retry),
        _ => unreachable!("only transfer actions have a primary control"),
    };
    icon_button(symbol, label, target, selector, mtm)
}

#[cfg(any(test, debug_assertions))]
fn icon_button(
    symbol: &str,
    accessibility_label: &str,
    target: Option<&AnyObject>,
    selector: Sel,
    mtm: MainThreadMarker,
) -> Retained<NSButton> {
    let button = NSButton::new(mtm);
    button.setTitle(&NSString::from_str(""));
    button.setBordered(false);
    button.setRefusesFirstResponder(true);
    button.setToolTip(Some(&NSString::from_str(accessibility_label)));
    button.setAccessibilityLabel(Some(&NSString::from_str(accessibility_label)));
    if let Some(image) = NSImage::imageWithSystemSymbolName_accessibilityDescription(
        &NSString::from_str(symbol),
        Some(&NSString::from_str(accessibility_label)),
    ) {
        button.setImage(Some(&image));
    }
    button.setContentTintColor(Some(&NSColor::secondaryLabelColor()));
    button.setTranslatesAutoresizingMaskIntoConstraints(false);
    activate(button.widthAnchor().constraintEqualToConstant(28.0));
    activate(button.heightAnchor().constraintEqualToConstant(28.0));
    // SAFETY: all selectors are implemented by the retained NativePopoverTarget.
    unsafe {
        button.setTarget(target);
        button.setAction(Some(selector));
    }
    button
}

fn text_button(
    title: &str,
    accessibility_label: &str,
    target: Option<&AnyObject>,
    selector: Sel,
    mtm: MainThreadMarker,
) -> Retained<NSButton> {
    let button = NSButton::new(mtm);
    button.setControlSize(NSControlSize::Small);
    button.setTitle(&NSString::from_str(title));
    button.setRefusesFirstResponder(true);
    button.setToolTip(Some(&NSString::from_str(accessibility_label)));
    button.setAccessibilityLabel(Some(&NSString::from_str(accessibility_label)));
    // SAFETY: all selectors are implemented by the retained NativePopoverTarget.
    unsafe {
        button.setTarget(target);
        button.setAction(Some(selector));
    }
    button
}

fn pin_to_content(root: &NSView, view: &NSView) {
    view.setTranslatesAutoresizingMaskIntoConstraints(false);
    root.addSubview(view);
    activate(
        view.leadingAnchor()
            .constraintEqualToAnchor_constant(&root.leadingAnchor(), CONTENT_INSET),
    );
    activate(
        view.trailingAnchor()
            .constraintEqualToAnchor_constant(&root.trailingAnchor(), -CONTENT_INSET),
    );
    activate(
        view.topAnchor()
            .constraintEqualToAnchor_constant(&root.topAnchor(), MenuLayout::VERTICAL_PADDING),
    );
    activate(
        view.bottomAnchor()
            .constraintEqualToAnchor_constant(&root.bottomAnchor(), -MenuLayout::VERTICAL_PADDING),
    );
}

fn activate(constraint: Retained<objc2_app_kit::NSLayoutConstraint>) {
    constraint.setActive(true);
}

fn set_label(label: &NSTextField, text: &str) {
    label.setStringValue(&NSString::from_str(text));
}

fn set_label_with_detail(label: &NSTextField, text: &str, detail: &str) {
    set_label(label, text);
    label.setToolTip(Some(&NSString::from_str(detail)));
    label.setAccessibilityLabel(Some(&NSString::from_str(&format!("{text}; {detail}"))));
}

fn apply_recommendation_size(label: &NSTextField, recommendation: &RecommendationRow) {
    match (recommendation.subtitle(), recommendation.size_detail()) {
        (Some(text), Some(detail)) => {
            set_label_with_detail(label, &text, &detail);
            label.setHidden(false);
        }
        (None, None) => {
            set_label(label, "");
            label.setToolTip(None);
            label.setAccessibilityLabel(None);
            label.setHidden(true);
        }
        _ => unreachable!("recommendation size text and detail stay paired"),
    }
}

fn footer_text(snapshot: &MenuSnapshot) -> String {
    snapshot.footer().label(env!("CARGO_PKG_VERSION"))
}

fn rect(x: f64, y: f64, width: f64, height: f64) -> NSRect {
    NSRect::new(NSPoint::new(x, y), NSSize::new(width, height))
}
