use objc2::rc::Retained;
use objc2::runtime::{AnyObject, Sel};
use objc2::{MainThreadMarker, MainThreadOnly};
use objc2_app_kit::{
    NSAccessibility, NSButton, NSColor, NSControlSize, NSFont, NSLineBreakMode, NSTextAlignment,
    NSTextField, NSView,
};
use objc2_foundation::{NSInteger, NSPoint, NSRect, NSSize, NSString};

use crate::menu::incomplete::{IncompleteItem, IncompleteState};
use crate::menu::presentation::MenuLayout;
use crate::menu::progress::format_bytes;

const WIDTH: f64 = MenuLayout::BASE_WIDTH;
const INSET: f64 = 16.0;
const ROW_HEIGHT: f64 = 58.0;
const CONFIRM_HEIGHT: f64 = 36.0;
const MESSAGE_HEIGHT: f64 = 24.0;

#[derive(Clone, Copy)]
pub(super) struct IncompleteActions {
    pub(super) prepare: Sel,
    pub(super) keep: Sel,
    pub(super) confirm: Sel,
}

pub(super) struct IncompleteContent {
    pub(super) view: Retained<NSView>,
    pub(super) height: f64,
    #[cfg(test)]
    pub(super) action_buttons: Vec<Retained<NSButton>>,
}

pub(super) fn content_height(state: &IncompleteState) -> f64 {
    ROW_HEIGHT * state.visible_items().len() as f64
        + if state.confirmation_model_id().is_some() {
            CONFIRM_HEIGHT
        } else {
            0.0
        }
        + if state.remaining_count() > 0 {
            MESSAGE_HEIGHT
        } else {
            0.0
        }
        + if state.inventory_error_message().is_some() {
            MESSAGE_HEIGHT
        } else {
            0.0
        }
        + if state.feedback_message().is_some() {
            MESSAGE_HEIGHT
        } else {
            0.0
        }
}

pub(super) fn build(
    state: &IncompleteState,
    target: Option<&AnyObject>,
    actions: IncompleteActions,
    mtm: MainThreadMarker,
) -> IncompleteContent {
    let height = content_height(state);
    let root = NSView::initWithFrame(NSView::alloc(mtm), rect(0.0, 0.0, WIDTH, height));
    let mut next_y = height;
    #[cfg(test)]
    let mut action_buttons = Vec::new();

    for (index, item) in state.visible_items().into_iter().enumerate() {
        let row = incomplete_row(item, index, state, target, actions.prepare, mtm);
        row.view
            .setFrame(rect(0.0, take(&mut next_y, ROW_HEIGHT), WIDTH, ROW_HEIGHT));
        root.addSubview(&row.view);
        #[cfg(test)]
        action_buttons.push(row.discard);

        if state.confirmation_model_id() == Some(item.id()) {
            let confirmation = row_shell(&mut next_y, CONFIRM_HEIGHT, mtm);
            let available = WIDTH - 2.0 * INSET;
            let keep = text_button(
                "Keep",
                "Keep the partial download",
                target,
                actions.keep,
                mtm,
            );
            keep.setFrame(rect(INSET, 4.0, available / 2.0 - 3.0, 28.0));
            confirmation.addSubview(&keep);
            let discard = text_button(
                "Discard partial",
                &format!("Discard the partial download for {}", item.id()),
                target,
                actions.confirm,
                mtm,
            );
            discard.setFrame(rect(
                INSET + available / 2.0 + 3.0,
                4.0,
                available / 2.0 - 3.0,
                28.0,
            ));
            confirmation.addSubview(&discard);
            root.addSubview(&confirmation);
            #[cfg(test)]
            action_buttons.extend([keep, discard]);
        }
    }

    if state.remaining_count() > 0 {
        root.addSubview(&message_row(
            &mut next_y,
            &format!("{} more — use loxa list", state.remaining_count()),
            mtm,
        ));
    }
    if let Some(message) = state.inventory_error_message() {
        root.addSubview(&message_row(&mut next_y, message, mtm));
    }
    if let Some(message) = state.feedback_message() {
        root.addSubview(&message_row(&mut next_y, message, mtm));
    }
    debug_assert_eq!(next_y, 0.0);
    IncompleteContent {
        view: root,
        height,
        #[cfg(test)]
        action_buttons,
    }
}

struct IncompleteRow {
    view: Retained<NSView>,
    #[cfg(test)]
    discard: Retained<NSButton>,
}

fn incomplete_row(
    item: &IncompleteItem,
    index: usize,
    state: &IncompleteState,
    target: Option<&AnyObject>,
    action: Sel,
    mtm: MainThreadMarker,
) -> IncompleteRow {
    let view = NSView::initWithFrame(NSView::alloc(mtm), rect(0.0, 0.0, WIDTH, ROW_HEIGHT));
    let primary = primary_label(item.id(), mtm);
    primary.setFrame(rect(INSET, 30.0, 230.0, 18.0));
    view.addSubview(&primary);
    let completed = item.completed_bytes().min(item.total_bytes());
    let percent = if item.total_bytes() == 0 {
        0
    } else {
        ((u128::from(completed) * 100 + u128::from(item.total_bytes()) / 2)
            / u128::from(item.total_bytes())) as u64
    };
    let detail = secondary_label(
        &format!(
            "{percent}% · {} of {}",
            format_bytes(completed),
            format_bytes(item.total_bytes())
        ),
        mtm,
    );
    detail.setFrame(rect(INSET, 9.0, 230.0, 16.0));
    view.addSubview(&detail);

    let (title, accessibility_label) = if state.is_preparing(item.id()) {
        (
            "Checking…",
            format!("Checking incomplete download {}", item.id()),
        )
    } else if state.is_discarding(item.id()) {
        (
            "Discarding…",
            format!("Discarding incomplete download {}", item.id()),
        )
    } else {
        (
            "Discard…",
            format!("Prepare to discard the partial download for {}", item.id()),
        )
    };
    let discard = text_button(title, &accessibility_label, target, action, mtm);
    discard.setTag(index as NSInteger);
    discard.setEnabled(!state.discard_is_active());
    discard.setFrame(rect(WIDTH - INSET - 88.0, 15.0, 88.0, 28.0));
    view.addSubview(&discard);
    IncompleteRow {
        view,
        #[cfg(test)]
        discard,
    }
}

fn message_row(next_y: &mut f64, text: &str, mtm: MainThreadMarker) -> Retained<NSView> {
    let root = row_shell(next_y, MESSAGE_HEIGHT, mtm);
    let label = secondary_label(text, mtm);
    label.setFrame(rect(INSET, 4.0, WIDTH - 2.0 * INSET, 16.0));
    root.addSubview(&label);
    root
}

fn row_shell(next_y: &mut f64, height: f64, mtm: MainThreadMarker) -> Retained<NSView> {
    NSView::initWithFrame(
        NSView::alloc(mtm),
        rect(0.0, take(next_y, height), WIDTH, height),
    )
}

fn take(next_y: &mut f64, height: f64) -> f64 {
    *next_y -= height;
    *next_y
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

fn rect(x: f64, y: f64, width: f64, height: f64) -> NSRect {
    NSRect::new(NSPoint::new(x, y), NSSize::new(width, height))
}
