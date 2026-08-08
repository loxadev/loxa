use objc2::rc::Retained;
use objc2::runtime::{AnyObject, Sel};
use objc2::{MainThreadMarker, MainThreadOnly};
use objc2_app_kit::{
    NSAccessibility, NSButton, NSControlSize, NSProgressIndicator, NSProgressIndicatorStyle,
    NSSearchField, NSTextField, NSView,
};
use objc2_foundation::{NSInteger, NSPoint, NSRect, NSSize, NSString};

use crate::menu::catalog::CatalogState;

const WIDTH: f64 = 300.0;
const INSET: f64 = 16.0;
const SEARCH_HEIGHT: f64 = 40.0;
const SECTION_HEIGHT: f64 = 28.0;
const RESULT_HEIGHT: f64 = 38.0;
const STATUS_HEIGHT: f64 = 44.0;
const ACTION_HEIGHT: f64 = 36.0;

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
    #[cfg(test)]
    pub(super) action_buttons: Vec<Retained<NSButton>>,
}

pub(super) fn content_height(state: &CatalogState) -> f64 {
    SEARCH_HEIGHT
        + STATUS_HEIGHT
        + if state.repositories().is_empty() {
            0.0
        } else {
            SECTION_HEIGHT + RESULT_HEIGHT * state.repositories().len() as f64
        }
        + if state.candidates().is_empty() {
            0.0
        } else {
            SECTION_HEIGHT + RESULT_HEIGHT * state.candidates().len() as f64
        }
        + if state.can_transfer() {
            ACTION_HEIGHT
        } else {
            0.0
        }
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

    let search_row = row(&mut next_y, SEARCH_HEIGHT, mtm);
    let search = NSSearchField::new(mtm);
    search.setFrame(rect(INSET, 6.0, WIDTH - 2.0 * INSET, 28.0));
    search.setStringValue(&NSString::from_str(state.query()));
    search.setPlaceholderString(Some(&NSString::from_str("Search Hugging Face")));
    search.setSendsWholeSearchString(true);
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

    if !state.repositories().is_empty() {
        root.addSubview(&section(&mut next_y, "Repositories", mtm));
        for (index, repository) in state.repositories().iter().enumerate() {
            let detail = repository.detail();
            let title = match detail {
                Some(detail) => format!("{} · {detail}", repository.repo()),
                None => repository.repo().into(),
            };
            let button = result_button(
                &title,
                repository.repo(),
                index,
                target,
                actions.repository,
                mtm,
            );
            let result = row(&mut next_y, RESULT_HEIGHT, mtm);
            button.setFrame(rect(INSET, 4.0, WIDTH - 2.0 * INSET, 30.0));
            result.addSubview(&button);
            root.addSubview(&result);
            #[cfg(test)]
            action_buttons.push(button);
        }
    }

    if !state.candidates().is_empty() {
        root.addSubview(&section(&mut next_y, "GGUF files", mtm));
        let selected = state.selected_candidate().map(|candidate| candidate.path());
        for (index, candidate) in state.candidates().iter().enumerate() {
            let marker = if selected == Some(candidate.path()) {
                "✓ "
            } else {
                ""
            };
            let size = candidate
                .size()
                .map(format_size)
                .unwrap_or_else(|| "Size unknown".into());
            let title = format!("{marker}{} · {size}", candidate.path());
            let button = result_button(
                &title,
                candidate.path(),
                index,
                target,
                actions.candidate,
                mtm,
            );
            let result = row(&mut next_y, RESULT_HEIGHT, mtm);
            button.setFrame(rect(INSET, 4.0, WIDTH - 2.0 * INSET, 30.0));
            result.addSubview(&button);
            root.addSubview(&result);
            #[cfg(test)]
            action_buttons.push(button);
        }
    }

    if state.can_transfer() {
        let action_row = row(&mut next_y, ACTION_HEIGHT, mtm);
        let transfer = text_button(
            "Download selected",
            "Download the explicitly selected GGUF file",
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

    let status_row = row(&mut next_y, STATUS_HEIGHT, mtm);
    let status_width = if state.can_pause() { 196.0 } else { 268.0 };
    let status = NSTextField::labelWithString(&NSString::from_str(&state.status_label()), mtm);
    status.setFrame(rect(INSET, 6.0, status_width, 18.0));
    status.setMaximumNumberOfLines(1);
    status.setToolTip(Some(&NSString::from_str(&state.status_label())));
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
        pause.setFrame(rect(220.0, 8.0, 64.0, 28.0));
        status_row.addSubview(&pause);
        #[cfg(test)]
        action_buttons.push(pause);
    }
    root.addSubview(&status_row);

    debug_assert_eq!(next_y, 0.0);
    CatalogContent {
        view: root,
        height,
        #[cfg(test)]
        action_buttons,
    }
}

fn section(next_y: &mut f64, title: &str, mtm: MainThreadMarker) -> Retained<NSView> {
    let root = row(next_y, SECTION_HEIGHT, mtm);
    let label = NSTextField::labelWithString(&NSString::from_str(title), mtm);
    label.setFrame(rect(INSET, 7.0, WIDTH - 2.0 * INSET, 18.0));
    root.addSubview(&label);
    root
}

fn row(next_y: &mut f64, height: f64, mtm: MainThreadMarker) -> Retained<NSView> {
    *next_y -= height;
    NSView::initWithFrame(NSView::alloc(mtm), rect(0.0, *next_y, WIDTH, height))
}

fn result_button(
    title: &str,
    accessibility_label: &str,
    index: usize,
    target: Option<&AnyObject>,
    action: Sel,
    mtm: MainThreadMarker,
) -> Retained<NSButton> {
    let button = text_button(title, accessibility_label, target, action, mtm);
    button.setBordered(false);
    button.setTag(index as NSInteger);
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
    button.setRefusesFirstResponder(true);
    button.setToolTip(Some(&NSString::from_str(accessibility_label)));
    button.setAccessibilityLabel(Some(&NSString::from_str(accessibility_label)));
    // SAFETY: NativePopoverTarget implements each selector retained by these controls.
    unsafe {
        button.setTarget(target);
        button.setAction(Some(action));
    }
    button
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
