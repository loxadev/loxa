use std::cell::RefCell;
use std::path::{Path, PathBuf};

use loxa::paths::AppPaths;
use objc2::rc::Retained;
use objc2::runtime::{AnyObject, Sel};
use objc2::{MainThreadMarker, MainThreadOnly};
use objc2_app_kit::{
    NSAccessibility, NSButton, NSColor, NSControlSize, NSFont, NSImage, NSImageView,
    NSLineBreakMode, NSPasteboard, NSPasteboardTypeString, NSScrollView, NSTextAlignment,
    NSTextField, NSView, NSWorkspace,
};
use objc2_foundation::{NSArray, NSInteger, NSPoint, NSRect, NSSize, NSString, NSURL};

use crate::menu::api_presentation::{ApiPresentation, ApiPrimaryAction, ApiPrimaryActionKind};
use crate::menu::installed::{InstalledFeedback, InstalledItem, InstalledState};
use crate::menu::presentation::MenuLayout;
use crate::menu::progress::format_bytes;

const WIDTH: f64 = MenuLayout::BASE_WIDTH;
const INSET: f64 = 16.0;
const ROW_HEIGHT: f64 = 52.0;
const ACTION_HEIGHT: f64 = 36.0;
const MESSAGE_HEIGHT: f64 = 24.0;
const ICON_SIZE: f64 = 16.0;
const VIEWPORT_ROWS: usize = 5;
const SECTION_HEIGHT: f64 = 28.0;

#[derive(Clone, Copy)]
pub(super) struct InstalledActions {
    pub(super) select: Sel,
    pub(super) api_start: Sel,
    pub(super) api_stop: Sel,
    pub(super) copy: Sel,
    pub(super) reveal: Sel,
}

pub(super) struct InstalledContent {
    pub(super) view: Retained<NSView>,
    pub(super) height: f64,
    pub(super) scroll: Option<Retained<NSScrollView>>,
    #[cfg(test)]
    #[allow(dead_code)] // Read by the include-based native layout harness.
    pub(super) row_buttons: Vec<Retained<NSButton>>,
    #[cfg(test)]
    pub(super) action_buttons: Vec<Retained<NSButton>>,
    #[cfg(test)]
    #[allow(dead_code)] // Read by the include-based native layout harness.
    pub(super) primary_labels: Vec<Retained<NSTextField>>,
    #[cfg(test)]
    #[allow(dead_code)] // Read by the include-based native layout harness.
    pub(super) secondary_labels: Vec<Retained<NSTextField>>,
}

pub(super) fn content_height(state: &InstalledState, api: &ApiPresentation) -> f64 {
    InstalledLayout::new(state, api).height()
}

pub(super) fn section_title(state: &InstalledState, api: &ApiPresentation) -> &'static str {
    if api
        .active_model_id()
        .and_then(|id| state.item(id))
        .is_some()
    {
        api.active_section_title().unwrap_or("Installed")
    } else {
        "Installed"
    }
}

struct InstalledLayout<'a> {
    active_id: Option<&'a str>,
    active_height: f64,
    list_header_height: f64,
    document_height: f64,
    viewport_height: f64,
}

impl<'a> InstalledLayout<'a> {
    fn new(state: &'a InstalledState, api: &ApiPresentation) -> Self {
        let active_id = api
            .active_model_id()
            .filter(|_| api.active_section_title().is_some())
            .and_then(|id| state.item(id))
            .map(|item| item.id());
        let selected = state.selected();
        let selected_height = selected.map_or(0.0, |item| {
            2.0 * ACTION_HEIGHT
                + if state.feedback_message().is_some() {
                    MESSAGE_HEIGHT
                } else {
                    0.0
                }
                + if api.primary_action(item.id()).disabled_reason().is_some() {
                    MESSAGE_HEIGHT
                } else {
                    0.0
                }
        });
        let active_height = active_id.map_or(0.0, |id| {
            ROW_HEIGHT
                + if selected.is_some_and(|item| item.id() == id) {
                    selected_height
                } else {
                    0.0
                }
        });
        let list_count = state.items().len() - usize::from(active_id.is_some());
        let document_height = state.items().len() as f64 * ROW_HEIGHT
            + selected_height
            + if state.error_message().is_some() {
                MESSAGE_HEIGHT
            } else {
                0.0
            }
            - active_height;
        let visible_rows = if active_id.is_some() {
            3
        } else {
            VIEWPORT_ROWS
        };
        Self {
            active_id,
            active_height,
            list_header_height: if active_id.is_some() && list_count > 0 {
                SECTION_HEIGHT
            } else {
                0.0
            },
            document_height,
            viewport_height: document_height
                - list_count.saturating_sub(visible_rows) as f64 * ROW_HEIGHT,
        }
    }

    fn height(&self) -> f64 {
        self.active_height + self.list_header_height + self.viewport_height
    }
}

pub(super) fn build(
    state: &InstalledState,
    api: &ApiPresentation,
    target: Option<&AnyObject>,
    actions: InstalledActions,
    mtm: MainThreadMarker,
) -> InstalledContent {
    let layout = InstalledLayout::new(state, api);
    let height = layout.height();
    let document_height = layout.document_height;
    let root = NSView::initWithFrame(NSView::alloc(mtm), rect(0.0, 0.0, WIDTH, document_height));
    let active = NSView::initWithFrame(
        NSView::alloc(mtm),
        rect(0.0, 0.0, WIDTH, layout.active_height),
    );
    let mut active_y = layout.active_height;
    let mut next_y = document_height;
    let mut selected_bounds = None;
    #[cfg(test)]
    let mut row_buttons = Vec::new();
    #[cfg(test)]
    let mut action_buttons = Vec::new();
    #[cfg(test)]
    let mut primary_labels = Vec::new();
    #[cfg(test)]
    let mut secondary_labels = Vec::new();

    let selected_id = state.selected().map(InstalledItem::id);
    for (index, item) in state
        .ordered_items_for(api.active_model_id())
        .into_iter()
        .enumerate()
    {
        let is_active = layout.active_id == Some(item.id());
        let (root, next_y) = if is_active {
            (&active, &mut active_y)
        } else {
            (&root, &mut next_y)
        };
        let row_top = *next_y;
        let selected = selected_id == Some(item.id());
        let result = installed_row(item, selected, index, target, actions.select, mtm);
        result
            .view
            .setFrame(rect(0.0, take(next_y, ROW_HEIGHT), WIDTH, ROW_HEIGHT));
        root.addSubview(&result.view);
        #[cfg(test)]
        {
            row_buttons.push(result.button);
            primary_labels.push(result.primary);
            secondary_labels.push(result.secondary);
        }
        if selected {
            let primary_action = api.primary_action(item.id());
            let primary_row = row(next_y, ACTION_HEIGHT, mtm);
            let primary = api_primary_button(primary_action, target, actions, mtm);
            primary.setFrame(rect(INSET, 4.0, WIDTH - 2.0 * INSET, 28.0));
            primary_row.addSubview(&primary);
            root.addSubview(&primary_row);
            #[cfg(test)]
            action_buttons.push(primary);

            if let Some(reason) = primary_action.disabled_reason() {
                root.addSubview(&message_row(next_y, reason, mtm));
            }

            let action_row = row(next_y, ACTION_HEIGHT, mtm);
            let available_width = WIDTH - 2.0 * INSET;
            let copy = text_button(
                "Copy chat command",
                if api.can_copy_chat_for(item.id()) {
                    "Copy chat command for the selected installed model"
                } else {
                    "Chat commands are unavailable in background service mode"
                },
                target,
                actions.copy,
                mtm,
            );
            copy.setEnabled(api.can_copy_chat_for(item.id()));
            copy.setFrame(rect(INSET, 4.0, available_width / 2.0 - 3.0, 28.0));
            action_row.addSubview(&copy);
            let reveal = text_button(
                "Reveal in Finder",
                "Reveal the selected installed model in Finder",
                target,
                actions.reveal,
                mtm,
            );
            reveal.setFrame(rect(
                INSET + available_width / 2.0 + 3.0,
                4.0,
                available_width / 2.0 - 3.0,
                28.0,
            ));
            action_row.addSubview(&reveal);
            root.addSubview(&action_row);
            #[cfg(test)]
            action_buttons.extend([copy, reveal]);
            if let Some(feedback) = state.feedback_message() {
                root.addSubview(&message_row(next_y, feedback, mtm));
            }
            if !is_active {
                selected_bounds = Some(rect(0.0, *next_y, WIDTH, row_top - *next_y));
            }
        }
    }
    if let Some(error) = state.error_message() {
        root.addSubview(&message_row(&mut next_y, error, mtm));
    }
    debug_assert_eq!(next_y, 0.0);
    debug_assert_eq!(active_y, 0.0);
    let viewport_height = layout.viewport_height;
    let scroll = (document_height > viewport_height).then(|| {
        let scroll = NSScrollView::initWithFrame(
            NSScrollView::alloc(mtm),
            rect(0.0, 0.0, WIDTH, viewport_height),
        );
        scroll.setDrawsBackground(false);
        scroll.setHasVerticalScroller(true);
        scroll.setAutohidesScrollers(true);
        scroll.setDocumentView(Some(&root));
        let clip = scroll.contentView();
        clip.scrollToPoint(NSPoint::new(0.0, document_height - viewport_height));
        scroll.reflectScrolledClipView(&clip);
        if let Some(bounds) = selected_bounds {
            root.scrollRectToVisible(bounds);
        }
        scroll
    });
    let list_view = scroll
        .as_ref()
        .map_or(root, |scroll| scroll.clone().into_super());
    let view = if layout.active_id.is_some() {
        let container = NSView::initWithFrame(NSView::alloc(mtm), rect(0.0, 0.0, WIDTH, height));
        active.setFrame(rect(
            0.0,
            height - layout.active_height,
            WIDTH,
            layout.active_height,
        ));
        container.addSubview(&active);
        if layout.list_header_height > 0.0 {
            let label = primary_label("Installed", mtm);
            label.setFrame(rect(
                INSET,
                viewport_height + 5.0,
                WIDTH - 2.0 * INSET,
                18.0,
            ));
            container.addSubview(&label);
        }
        container.addSubview(&list_view);
        container
    } else {
        list_view
    };
    InstalledContent {
        view,
        height,
        scroll,
        #[cfg(test)]
        row_buttons,
        #[cfg(test)]
        action_buttons,
        #[cfg(test)]
        primary_labels,
        #[cfg(test)]
        secondary_labels,
    }
}

impl InstalledContent {
    pub(super) fn scroll_offset(&self) -> Option<f64> {
        let scroll = self.scroll.as_ref()?;
        let document = scroll.documentView()?;
        let clip = scroll.contentView().bounds();
        Some((document.frame().size.height - clip.origin.y - clip.size.height).max(0.0))
    }

    pub(super) fn restore_scroll_offset(&self, offset: f64) {
        let Some(scroll) = &self.scroll else { return };
        let Some(document) = scroll.documentView() else {
            return;
        };
        let clip = scroll.contentView();
        let max_y = (document.frame().size.height - clip.bounds().size.height).max(0.0);
        clip.scrollToPoint(NSPoint::new(0.0, (max_y - offset).clamp(0.0, max_y)));
        scroll.reflectScrolledClipView(&clip);
    }
}

fn api_primary_button(
    action: ApiPrimaryAction,
    target: Option<&AnyObject>,
    actions: InstalledActions,
    mtm: MainThreadMarker,
) -> Retained<NSButton> {
    let selector = match action.kind() {
        ApiPrimaryActionKind::Start => actions.api_start,
        ApiPrimaryActionKind::Stop => actions.api_stop,
    };
    let accessibility_label = action.disabled_reason().unwrap_or(action.title());
    let button = text_button(action.title(), accessibility_label, target, selector, mtm);
    button.setEnabled(action.is_enabled());
    button
}

struct InstalledRow {
    view: Retained<NSView>,
    #[cfg(test)]
    button: Retained<NSButton>,
    #[cfg(test)]
    primary: Retained<NSTextField>,
    #[cfg(test)]
    secondary: Retained<NSTextField>,
}

fn installed_row(
    item: &InstalledItem,
    selected: bool,
    index: usize,
    target: Option<&AnyObject>,
    action: Sel,
    mtm: MainThreadMarker,
) -> InstalledRow {
    let view = NSView::initWithFrame(NSView::alloc(mtm), rect(0.0, 0.0, WIDTH, ROW_HEIGHT));
    let leading = image_view("internaldrive", mtm);
    leading.setFrame(rect(INSET, 18.0, ICON_SIZE, ICON_SIZE));
    view.addSubview(&leading);

    let label_x = INSET + ICON_SIZE + 10.0;
    let label_width = WIDTH - label_x - INSET - ICON_SIZE - 10.0;
    let display_name = item
        .display_name()
        .strip_suffix(".gguf")
        .unwrap_or(item.display_name());
    let primary = primary_label(display_name, mtm);
    primary.setFrame(rect(label_x, 27.0, label_width, 18.0));
    primary.setToolTip(Some(&NSString::from_str(item.display_name())));
    view.addSubview(&primary);

    let secondary_text = format!("{} · {}", item.id(), format_bytes(item.total_bytes()));
    let secondary = secondary_label(&secondary_text, mtm);
    secondary.setFrame(rect(label_x, 8.0, label_width, 16.0));
    view.addSubview(&secondary);

    let trailing = image_view(
        if selected {
            "checkmark.circle.fill"
        } else {
            "chevron.right"
        },
        mtm,
    );
    trailing.setFrame(rect(WIDTH - INSET - ICON_SIZE, 18.0, ICON_SIZE, ICON_SIZE));
    view.addSubview(&trailing);

    let label = if selected {
        format!("{}; selected; actions expanded", item.id())
    } else {
        format!("{}; select installed model", item.id())
    };
    let button = action_button(&label, index, target, action, mtm);
    button.setFrame(rect(INSET, 2.0, WIDTH - 2.0 * INSET, ROW_HEIGHT - 4.0));
    view.addSubview(&button);

    InstalledRow {
        view,
        #[cfg(test)]
        button,
        #[cfg(test)]
        primary,
        #[cfg(test)]
        secondary,
    }
}

fn message_row(next_y: &mut f64, text: &str, mtm: MainThreadMarker) -> Retained<NSView> {
    let root = row(next_y, MESSAGE_HEIGHT, mtm);
    let label = secondary_label(text, mtm);
    label.setFrame(rect(INSET, 4.0, WIDTH - 2.0 * INSET, 16.0));
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

fn action_button(
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

fn image_view(symbol: &str, mtm: MainThreadMarker) -> Retained<NSImageView> {
    let image_view = NSImageView::new(mtm);
    if let Some(image) = NSImage::imageWithSystemSymbolName_accessibilityDescription(
        &NSString::from_str(symbol),
        None,
    ) {
        image_view.setImage(Some(&image));
    }
    image_view.setContentTintColor(Some(&NSColor::secondaryLabelColor()));
    image_view.setAccessibilityElement(false);
    image_view
}

fn rect(x: f64, y: f64, width: f64, height: f64) -> NSRect {
    NSRect::new(NSPoint::new(x, y), NSSize::new(width, height))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum InstalledAction {
    CopyChatCommand,
    RevealInFinder,
}

pub(super) fn dispatch_native_selected_action(
    state: &RefCell<InstalledState>,
    action: InstalledAction,
    paths: &AppPaths,
    allow_copy_chat: bool,
) {
    if action == InstalledAction::CopyChatCommand && !allow_copy_chat {
        return;
    }
    dispatch_selected_action(
        state,
        action,
        |model_id| paths.model_dir(model_id).map_err(|_| ()),
        |path| std::fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_dir()),
        |command| {
            let pasteboard = NSPasteboard::generalPasteboard();
            pasteboard.clearContents();
            // SAFETY: AppKit initializes this immutable standard pasteboard type.
            let string_type = unsafe { NSPasteboardTypeString };
            pasteboard
                .setString_forType(&NSString::from_str(command), string_type)
                .then_some(())
                .ok_or(())
        },
        |path| {
            let path = NSString::from_str(path.to_str().ok_or(())?);
            let url = NSURL::fileURLWithPath_isDirectory(&path, true);
            let urls = NSArray::from_retained_slice(&[url]);
            NSWorkspace::sharedWorkspace().activateFileViewerSelectingURLs(&urls);
            Ok(())
        },
    );
}

pub(super) fn dispatch_selected_action<Resolve, SafeDirectory, Copy, Reveal>(
    state: &RefCell<InstalledState>,
    action: InstalledAction,
    resolve_model_dir: Resolve,
    is_safe_directory: SafeDirectory,
    copy: Copy,
    reveal: Reveal,
) where
    Resolve: FnOnce(&str) -> Result<PathBuf, ()>,
    SafeDirectory: FnOnce(&Path) -> bool,
    Copy: FnOnce(&str) -> Result<(), ()>,
    Reveal: FnOnce(&Path) -> Result<(), ()>,
{
    let Some(model_id) = state
        .borrow()
        .selected()
        .map(InstalledItem::id)
        .map(str::to_owned)
    else {
        return;
    };

    let feedback = match action {
        InstalledAction::CopyChatCommand => {
            let command = format!("loxa chat '{model_id}'");
            match copy(&command) {
                Ok(()) => Some(InstalledFeedback::ChatCommandCopied),
                Err(()) => Some(InstalledFeedback::CopyFailed),
            }
        }
        InstalledAction::RevealInFinder => match resolve_model_dir(&model_id) {
            Ok(path) if is_safe_directory(&path) => match reveal(&path) {
                Ok(()) => None,
                Err(()) => Some(InstalledFeedback::RevealFailed),
            },
            Ok(_) | Err(()) => Some(InstalledFeedback::RevealFailed),
        },
    };

    state.borrow_mut().apply_feedback(&model_id, feedback);
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::path::{Path, PathBuf};

    use super::{dispatch_selected_action, InstalledAction};
    use crate::menu::installed::{InstalledItem, InstalledState};

    fn selected_state() -> RefCell<InstalledState> {
        let mut state = InstalledState::default();
        state.replace(
            vec![
                InstalledItem::new("alpha".into(), "alpha-q4.gguf".into(), 88_200_000),
                InstalledItem::new("beta".into(), "beta-q8.gguf".into(), 99_000_000),
            ],
            None,
        );
        assert!(state.select("alpha"));
        RefCell::new(state)
    }

    #[test]
    fn copy_action_uses_the_exact_chat_command_without_holding_the_state_borrow() {
        let state = selected_state();
        let copied = RefCell::new(None::<String>);

        dispatch_selected_action(
            &state,
            InstalledAction::CopyChatCommand,
            |id| {
                assert_eq!(id, "alpha");
                Ok(PathBuf::from("/isolated/models/alpha"))
            },
            |_| panic!("copy must not inspect a model directory"),
            |command| {
                state.borrow_mut().reset_feedback();
                *copied.borrow_mut() = Some(command.into());
                Ok(())
            },
            |_| panic!("copy must not invoke Finder"),
        );

        assert_eq!(copied.into_inner().as_deref(), Some("loxa chat 'alpha'"));
        assert_eq!(
            state.borrow().feedback_message(),
            Some("Chat command copied")
        );
    }

    #[test]
    fn copy_action_succeeds_when_model_directory_resolution_would_fail() {
        let state = selected_state();
        let copied = RefCell::new(None::<String>);

        dispatch_selected_action(
            &state,
            InstalledAction::CopyChatCommand,
            |_| Err(()),
            |_| panic!("copy must not inspect a model directory"),
            |command| {
                *copied.borrow_mut() = Some(command.into());
                Ok(())
            },
            |_| panic!("copy must not invoke Finder"),
        );

        assert_eq!(copied.into_inner().as_deref(), Some("loxa chat 'alpha'"));
        assert_eq!(
            state.borrow().feedback_message(),
            Some("Chat command copied")
        );
    }

    #[test]
    fn action_feedback_is_static_and_only_reapplies_to_the_same_selection() {
        let state = selected_state();
        dispatch_selected_action(
            &state,
            InstalledAction::CopyChatCommand,
            |_| Ok(PathBuf::from("/isolated/models/alpha")),
            |_| false,
            |_| {
                assert!(state.borrow_mut().select("beta"));
                Ok(())
            },
            |_| unreachable!(),
        );
        assert_eq!(
            state.borrow().selected().map(InstalledItem::id),
            Some("beta")
        );
        assert_eq!(state.borrow().feedback_message(), None);

        assert!(state.borrow_mut().select("alpha"));
        dispatch_selected_action(
            &state,
            InstalledAction::CopyChatCommand,
            |_| Ok(PathBuf::from("/isolated/models/alpha")),
            |_| false,
            |_| Err(()),
            |_| unreachable!(),
        );
        assert_eq!(
            state.borrow().feedback_message(),
            Some("Could not copy the chat command")
        );

        assert!(state.borrow_mut().select("beta"));
        assert_eq!(state.borrow().feedback_message(), None);
        state.borrow_mut().reset_feedback();
        assert_eq!(state.borrow().feedback_message(), None);
    }

    #[test]
    fn reveal_action_requires_the_resolved_safe_directory_and_never_exposes_its_path() {
        let state = selected_state();
        let expected = Path::new("/isolated/models/alpha");
        let revealed = RefCell::new(None::<PathBuf>);

        dispatch_selected_action(
            &state,
            InstalledAction::RevealInFinder,
            |_| Ok(expected.to_path_buf()),
            |path| path == expected,
            |_| panic!("reveal must not touch the clipboard"),
            |path| {
                *revealed.borrow_mut() = Some(path.to_path_buf());
                Ok(())
            },
        );
        assert_eq!(revealed.into_inner().as_deref(), Some(expected));
        assert_eq!(state.borrow().feedback_message(), None);

        dispatch_selected_action(
            &state,
            InstalledAction::RevealInFinder,
            |_| Ok(expected.to_path_buf()),
            |_| false,
            |_| unreachable!(),
            |_| panic!("an unsafe directory must never reach Finder"),
        );
        assert_eq!(
            state.borrow().feedback_message(),
            Some("Could not reveal this model")
        );

        dispatch_selected_action(
            &state,
            InstalledAction::RevealInFinder,
            |_| Err(()),
            |_| panic!("an unresolved model must not be inspected"),
            |_| unreachable!(),
            |_| panic!("an unresolved model must never reach Finder"),
        );
        assert_eq!(
            state.borrow().feedback_message(),
            Some("Could not reveal this model")
        );
    }
}
