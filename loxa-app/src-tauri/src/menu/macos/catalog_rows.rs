use objc2::rc::Retained;
use objc2::runtime::{AnyObject, Sel};
use objc2::{MainThreadMarker, MainThreadOnly};
use objc2_app_kit::{
    NSAccessibility, NSButton, NSColor, NSControlSize, NSFont, NSImage, NSImageView,
    NSLineBreakMode, NSProgressIndicator, NSProgressIndicatorStyle, NSSearchField, NSTextAlignment,
    NSTextField, NSView, NSWindow,
};
use objc2_foundation::{NSInteger, NSPoint, NSRange, NSRect, NSSize, NSString};

use crate::menu::catalog::{CandidateItem, CatalogState, RepositoryItem};
use crate::menu::presentation::MenuLayout;

const WIDTH: f64 = MenuLayout::BASE_WIDTH;
const INSET: f64 = 16.0;
const SEARCH_HEIGHT: f64 = 40.0;
const SECTION_HEIGHT: f64 = 28.0;
const RESULT_HEIGHT: f64 = 48.0;
const STATUS_HEIGHT: f64 = 44.0;
const ACTION_HEIGHT: f64 = 36.0;
const ICON_SIZE: f64 = 16.0;

#[derive(Clone, Copy)]
pub(super) struct CatalogActions {
    pub(super) search: Sel,
    pub(super) repository: Sel,
    pub(super) candidate: Sel,
    pub(super) transfer: Sel,
    pub(super) pause: Sel,
}

pub(super) struct CatalogContent {
    pub(super) view: Retained<NSView>,
    pub(super) height: f64,
    pub(super) search: Retained<NSSearchField>,
    #[cfg(test)]
    pub(super) action_buttons: Vec<Retained<NSButton>>,
    #[cfg(test)]
    #[allow(dead_code)] // Read by the include-based native layout harness.
    pub(super) primary_labels: Vec<Retained<NSTextField>>,
    #[cfg(test)]
    #[allow(dead_code)] // Read by the include-based native layout harness.
    pub(super) secondary_labels: Vec<Retained<NSTextField>>,
}

pub(super) struct SearchFocus {
    window: Retained<NSWindow>,
    selected_range: NSRange,
}

impl CatalogContent {
    pub(super) fn capture_search_focus(&self) -> Option<SearchFocus> {
        let editor = self.search.currentEditor()?;
        let window = self.search.window()?;
        Some(SearchFocus {
            window,
            selected_range: editor.selectedRange(),
        })
    }

    pub(super) fn restore_search_focus(&self, focus: SearchFocus) {
        if !focus.window.makeFirstResponder(Some(&self.search)) {
            return;
        }
        if let Some(editor) = self.search.currentEditor() {
            editor.setSelectedRange(focus.selected_range);
        }
    }
}

pub(super) fn content_height(state: &CatalogState) -> f64 {
    SEARCH_HEIGHT
        + state.browser_heading().map_or(0.0, |_| SECTION_HEIGHT)
        + if state.shows_repository_rows() {
            RESULT_HEIGHT * state.repositories().len() as f64
        } else {
            0.0
        }
        + if state.shows_candidate_rows() {
            RESULT_HEIGHT * state.candidates().len() as f64
        } else {
            0.0
        }
        + if state.can_transfer() {
            ACTION_HEIGHT
        } else {
            0.0
        }
        + state.browser_status().map_or(0.0, |_| STATUS_HEIGHT)
}

pub(super) fn build(
    state: &CatalogState,
    target: Option<&AnyObject>,
    actions: CatalogActions,
    mtm: MainThreadMarker,
) -> CatalogContent {
    let height = content_height(state);
    let root = NSView::initWithFrame(NSView::alloc(mtm), rect(0.0, 0.0, WIDTH, height));
    let mut next_y = height;
    #[cfg(test)]
    let mut action_buttons = Vec::new();
    #[cfg(test)]
    let mut primary_labels = Vec::new();
    #[cfg(test)]
    let mut secondary_labels = Vec::new();

    let search_row = row(&mut next_y, SEARCH_HEIGHT, mtm);
    let search = NSSearchField::new(mtm);
    search.setFrame(rect(INSET, 6.0, WIDTH - 2.0 * INSET, 28.0));
    search.setStringValue(&NSString::from_str(state.query()));
    search.setPlaceholderString(Some(&NSString::from_str("Search Hugging Face")));
    search.setSendsWholeSearchString(false);
    search.setSendsSearchStringImmediately(false);
    search.setToolTip(Some(&NSString::from_str(
        "Search by model name or owner/repository",
    )));
    search.setAccessibilityLabel(Some(&NSString::from_str("Search Hugging Face models")));
    // SAFETY: NativePopoverTarget owns the retained control and implements this selector.
    unsafe {
        search.setTarget(target);
        search.setAction(Some(actions.search));
    }
    search_row.addSubview(&search);
    root.addSubview(&search_row);

    if let Some(heading) = state.browser_heading() {
        root.addSubview(&section(&mut next_y, heading, mtm));
    }

    if state.shows_repository_rows() {
        for (index, repository) in state.repositories().iter().enumerate() {
            let result = repository_row(repository, index, target, actions.repository, mtm);
            result.view.setFrame(rect(
                0.0,
                take(&mut next_y, RESULT_HEIGHT),
                WIDTH,
                RESULT_HEIGHT,
            ));
            root.addSubview(&result.view);
            #[cfg(test)]
            {
                action_buttons.push(result.button);
                primary_labels.push(result.primary);
                secondary_labels.push(result.secondary);
            }
        }
    }

    if state.shows_candidate_rows() {
        let selected = state.selected_candidate().map(CandidateItem::path);
        for (index, candidate) in state.candidates().iter().enumerate() {
            let result = candidate_row(
                candidate,
                selected == Some(candidate.path()),
                index,
                target,
                actions.candidate,
                mtm,
            );
            result.view.setFrame(rect(
                0.0,
                take(&mut next_y, RESULT_HEIGHT),
                WIDTH,
                RESULT_HEIGHT,
            ));
            root.addSubview(&result.view);
            #[cfg(test)]
            {
                action_buttons.push(result.button);
                primary_labels.push(result.primary);
                secondary_labels.push(result.secondary);
            }
        }
    }

    if let Some(title) = state.transfer_action_label() {
        let action_row = row(&mut next_y, ACTION_HEIGHT, mtm);
        let transfer = text_button(
            &title,
            "Download or check the explicitly selected GGUF file",
            target,
            actions.transfer,
            mtm,
        );
        transfer.setFrame(rect(INSET, 4.0, WIDTH - 2.0 * INSET, 28.0));
        action_row.addSubview(&transfer);
        root.addSubview(&action_row);
        #[cfg(test)]
        action_buttons.push(transfer);
    }

    if let Some(status_text) = state.browser_status() {
        let status_row = row(&mut next_y, STATUS_HEIGHT, mtm);
        let status_width = if state.can_pause() {
            WIDTH - 2.0 * INSET - 72.0
        } else {
            WIDTH - 2.0 * INSET
        };
        let status = secondary_label(&status_text, mtm);
        status.setFrame(rect(INSET, 6.0, status_width, 18.0));
        status.setToolTip(Some(&NSString::from_str(&status_text)));
        status_row.addSubview(&status);
        if let Some(fraction) = state.progress_fraction() {
            let progress = NSProgressIndicator::initWithFrame(
                NSProgressIndicator::alloc(mtm),
                rect(INSET, 28.0, status_width, 8.0),
            );
            progress.setIndeterminate(false);
            progress.setMinValue(0.0);
            progress.setMaxValue(1.0);
            progress.setDoubleValue(fraction);
            progress.setStyle(NSProgressIndicatorStyle::Bar);
            status_row.addSubview(&progress);
        }
        if state.can_pause() {
            let pause = text_button(
                "Pause",
                "Pause this model download",
                target,
                actions.pause,
                mtm,
            );
            pause.setFrame(rect(WIDTH - INSET - 64.0, 8.0, 64.0, 28.0));
            status_row.addSubview(&pause);
            #[cfg(test)]
            action_buttons.push(pause);
        }
        root.addSubview(&status_row);
    }

    debug_assert_eq!(next_y, 0.0);
    CatalogContent {
        view: root,
        height,
        search,
        #[cfg(test)]
        action_buttons,
        #[cfg(test)]
        primary_labels,
        #[cfg(test)]
        secondary_labels,
    }
}

struct ResultRow {
    view: Retained<NSView>,
    #[cfg(test)]
    button: Retained<NSButton>,
    #[cfg(test)]
    primary: Retained<NSTextField>,
    #[cfg(test)]
    secondary: Retained<NSTextField>,
}

fn repository_row(
    repository: &RepositoryItem,
    index: usize,
    target: Option<&AnyObject>,
    action: Sel,
    mtm: MainThreadMarker,
) -> ResultRow {
    result_row(
        repository.repo(),
        repository.detail().unwrap_or("Repository"),
        "cube.box",
        "chevron.right",
        repository.repo(),
        index,
        target,
        action,
        mtm,
    )
}

fn candidate_row(
    candidate: &CandidateItem,
    selected: bool,
    index: usize,
    target: Option<&AnyObject>,
    action: Sel,
    mtm: MainThreadMarker,
) -> ResultRow {
    let mut detail = candidate
        .size()
        .map(format_size)
        .unwrap_or_else(|| "Size unknown".into());
    if candidate.is_installed() {
        detail.push_str(" · Installed");
    }
    result_row(
        candidate.path(),
        &detail,
        "doc",
        if selected {
            "checkmark.circle.fill"
        } else {
            "chevron.right"
        },
        candidate.path(),
        index,
        target,
        action,
        mtm,
    )
}

#[allow(clippy::too_many_arguments)]
fn result_row(
    primary_text: &str,
    secondary_text: &str,
    leading_symbol: &str,
    trailing_symbol: &str,
    accessibility_label: &str,
    index: usize,
    target: Option<&AnyObject>,
    action: Sel,
    mtm: MainThreadMarker,
) -> ResultRow {
    let view = NSView::initWithFrame(NSView::alloc(mtm), rect(0.0, 0.0, WIDTH, RESULT_HEIGHT));
    let leading = image_view(leading_symbol, None, mtm);
    leading.setFrame(rect(INSET, 16.0, ICON_SIZE, ICON_SIZE));
    view.addSubview(&leading);

    let label_x = INSET + ICON_SIZE + 10.0;
    let label_width = WIDTH - label_x - INSET - ICON_SIZE - 10.0;
    let primary = primary_label(primary_text, mtm);
    primary.setFrame(rect(label_x, 24.0, label_width, 18.0));
    primary.setToolTip(Some(&NSString::from_str(primary_text)));
    view.addSubview(&primary);
    let secondary = secondary_label(secondary_text, mtm);
    secondary.setFrame(rect(label_x, 7.0, label_width, 16.0));
    view.addSubview(&secondary);

    let trailing = image_view(trailing_symbol, None, mtm);
    trailing.setFrame(rect(WIDTH - INSET - ICON_SIZE, 16.0, ICON_SIZE, ICON_SIZE));
    view.addSubview(&trailing);

    let button = result_button(accessibility_label, index, target, action, mtm);
    button.setFrame(rect(INSET, 2.0, WIDTH - 2.0 * INSET, RESULT_HEIGHT - 4.0));
    view.addSubview(&button);
    ResultRow {
        view,
        #[cfg(test)]
        button,
        #[cfg(test)]
        primary,
        #[cfg(test)]
        secondary,
    }
}

fn section(next_y: &mut f64, title: &str, mtm: MainThreadMarker) -> Retained<NSView> {
    let root = row(next_y, SECTION_HEIGHT, mtm);
    let label = primary_label(title, mtm);
    label.setFrame(rect(INSET, 7.0, WIDTH - 2.0 * INSET, 18.0));
    root.addSubview(&label);
    root
}

fn row(next_y: &mut f64, height: f64, mtm: MainThreadMarker) -> Retained<NSView> {
    NSView::initWithFrame(
        NSView::alloc(mtm),
        rect(0.0, take(next_y, height), WIDTH, height),
    )
}

fn take(next_y: &mut f64, height: f64) -> f64 {
    *next_y -= height;
    *next_y
}

fn result_button(
    accessibility_label: &str,
    index: usize,
    target: Option<&AnyObject>,
    action: Sel,
    mtm: MainThreadMarker,
) -> Retained<NSButton> {
    let button = NSButton::new(mtm);
    button.setTitle(&NSString::from_str(""));
    button.setBordered(false);
    button.setRefusesFirstResponder(false);
    button.setTag(index as NSInteger);
    button.setToolTip(Some(&NSString::from_str(accessibility_label)));
    button.setAccessibilityLabel(Some(&NSString::from_str(accessibility_label)));
    // SAFETY: NativePopoverTarget implements the selector retained by this control.
    unsafe {
        button.setTarget(target);
        button.setAction(Some(action));
    }
    button
}

fn text_button(
    title: &str,
    accessibility_label: &str,
    target: Option<&AnyObject>,
    action: Sel,
    mtm: MainThreadMarker,
) -> Retained<NSButton> {
    let button = NSButton::new(mtm);
    button.setControlSize(NSControlSize::Small);
    button.setTitle(&NSString::from_str(title));
    button.setRefusesFirstResponder(false);
    button.setToolTip(Some(&NSString::from_str(accessibility_label)));
    button.setAccessibilityLabel(Some(&NSString::from_str(accessibility_label)));
    // SAFETY: NativePopoverTarget implements the selector retained by this control.
    unsafe {
        button.setTarget(target);
        button.setAction(Some(action));
    }
    button
}

fn primary_label(text: &str, mtm: MainThreadMarker) -> Retained<NSTextField> {
    let label = label(
        text,
        MenuLayout::primary_font_size(),
        NSColor::labelColor(),
        mtm,
    );
    label.setAlignment(NSTextAlignment::Left);
    if let Some(cell) = label.cell() {
        cell.setLineBreakMode(NSLineBreakMode::ByTruncatingMiddle);
    }
    label
}

fn secondary_label(text: &str, mtm: MainThreadMarker) -> Retained<NSTextField> {
    label(
        text,
        MenuLayout::secondary_font_size(),
        NSColor::secondaryLabelColor(),
        mtm,
    )
}

fn label(
    text: &str,
    size: f64,
    color: Retained<NSColor>,
    mtm: MainThreadMarker,
) -> Retained<NSTextField> {
    let label = NSTextField::labelWithString(&NSString::from_str(text), mtm);
    label.setFont(Some(&NSFont::systemFontOfSize(size)));
    label.setTextColor(Some(&color));
    label.setMaximumNumberOfLines(1);
    label
}

fn image_view(
    symbol: &str,
    description: Option<&str>,
    mtm: MainThreadMarker,
) -> Retained<NSImageView> {
    let image_view = NSImageView::new(mtm);
    let description = description.map(NSString::from_str);
    if let Some(image) = NSImage::imageWithSystemSymbolName_accessibilityDescription(
        &NSString::from_str(symbol),
        description.as_deref(),
    ) {
        image_view.setImage(Some(&image));
    }
    image_view.setContentTintColor(Some(&NSColor::secondaryLabelColor()));
    if description.is_none() {
        image_view.setAccessibilityElement(false);
    }
    image_view
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

fn rect(x: f64, y: f64, width: f64, height: f64) -> NSRect {
    NSRect::new(NSPoint::new(x, y), NSSize::new(width, height))
}
