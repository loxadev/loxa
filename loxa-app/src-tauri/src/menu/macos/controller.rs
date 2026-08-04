use std::cell::RefCell;
use std::rc::Rc;

use objc2::rc::Retained;
use objc2::runtime::{AnyObject, ProtocolObject};
use objc2::{define_class, msg_send, sel, DefinedClass, MainThreadMarker, MainThreadOnly};
use objc2_app_kit::{
    NSAccessibility, NSPopover, NSPopoverBehavior, NSPopoverDelegate, NSStatusItem,
    NSViewController,
};
use objc2_foundation::{NSNotification, NSObject, NSObjectProtocol, NSRectEdge, NSString};
use tauri::AppHandle;

use super::rows::{Actions, MenuRows, PopoverContent};
use crate::menu::presentation::{Fixture, InlineCancelState, MenuAction, MenuSnapshot, MenuUpdate};

struct NativePopoverState {
    status_item: Retained<NSStatusItem>,
    popover: Retained<NSPopover>,
    content_view_controller: Retained<NSViewController>,
    snapshot: MenuSnapshot,
    rendered: Option<MenuSnapshot>,
    cancel: InlineCancelState,
    rows: Option<MenuRows>,
}

impl NativePopoverState {
    fn new(
        status_item: Retained<NSStatusItem>,
        popover: Retained<NSPopover>,
        content_view_controller: Retained<NSViewController>,
        fixture: Fixture,
    ) -> Self {
        Self {
            status_item,
            popover,
            content_view_controller,
            snapshot: fixture.snapshot(),
            rendered: None,
            cancel: InlineCancelState::default(),
            rows: None,
        }
    }

    fn render(&mut self, target: &AnyObject, actions: Actions, mtm: MainThreadMarker) {
        match self.snapshot.update_from(self.rendered.as_ref()) {
            MenuUpdate::Rebuild => self.rebuild(target, actions, mtm),
            MenuUpdate::UpdateRetainedRows => {
                if let Some(rows) = &mut self.rows {
                    rows.update(&self.snapshot, &self.cancel);
                } else {
                    self.rebuild(target, actions, mtm);
                }
            }
        }
        self.rendered = Some(self.snapshot.clone());
    }

    fn rebuild(&mut self, target: &AnyObject, actions: Actions, mtm: MainThreadMarker) {
        let PopoverContent { view, rows, .. } =
            MenuRows::build(&self.snapshot, Some(target), actions, mtm);
        self.popover.setContentSize(view.frame().size);
        self.content_view_controller.setView(&view);
        self.rows = Some(rows);
    }

    fn popover_closed(&mut self) {
        self.cancel.reset();
        if let Some(rows) = &mut self.rows {
            rows.update_cancel_controls(&self.cancel);
        }
    }

    fn apply_fixture_action(&mut self, action: MenuAction) -> bool {
        let Some(snapshot) = self.snapshot.apply_fixture_action(action) else {
            return false;
        };
        self.snapshot = snapshot;
        self.cancel.reset();
        true
    }

    fn show_cancel_confirmation(&mut self) {
        if let Some(rows) = &mut self.rows {
            rows.show_cancel_confirmation(&mut self.cancel);
        } else {
            self.cancel.activate_cancel();
        }
    }

    fn keep_partial_fixture(&mut self) {
        self.cancel.keep_partial();
        self.update_cancel_controls();
    }

    fn discard_partial_fixture(&mut self) -> bool {
        if !self.cancel.discard_partial() {
            return false;
        }

        // This only changes the injected fixture displayed by the native popover.
        // It never touches a managed artifact or starts a background operation.
        self.snapshot = Fixture::Empty.snapshot();
        true
    }

    fn update_cancel_controls(&mut self) {
        if let Some(rows) = &mut self.rows {
            rows.update_cancel_controls(&self.cancel);
        }
    }
}

struct NativePopoverDelegateIvars {
    state: Rc<RefCell<NativePopoverState>>,
}

define_class!(
    // SAFETY: NSObject has no subclassing requirements, and this class has no Drop implementation.
    #[unsafe(super = NSObject)]
    #[thread_kind = MainThreadOnly]
    #[ivars = NativePopoverDelegateIvars]
    struct NativePopoverDelegate;

    // SAFETY: NSObjectProtocol has no safety requirements.
    unsafe impl NSObjectProtocol for NativePopoverDelegate {}

    // SAFETY: NSPopoverDelegate has no safety requirements.
    unsafe impl NSPopoverDelegate for NativePopoverDelegate {
        #[allow(non_snake_case)]
        #[unsafe(method(popoverDidClose:))]
        fn popoverDidClose(&self, _notification: &NSNotification) {
            self.ivars().state.borrow_mut().popover_closed();
        }
    }
);

impl NativePopoverDelegate {
    fn new(state: Rc<RefCell<NativePopoverState>>, mtm: MainThreadMarker) -> Retained<Self> {
        let this = Self::alloc(mtm).set_ivars(NativePopoverDelegateIvars { state });

        // SAFETY: NSObject's init selector has the expected signature.
        unsafe { msg_send![super(this), init] }
    }
}

struct NativePopoverTargetIvars {
    app_handle: AppHandle,
    state: Rc<RefCell<NativePopoverState>>,
}

define_class!(
    // SAFETY: NSObject has no subclassing requirements, and this class has no Drop implementation.
    #[unsafe(super = NSObject)]
    #[thread_kind = MainThreadOnly]
    #[ivars = NativePopoverTargetIvars]
    struct NativePopoverTarget;

    // SAFETY: NSObjectProtocol has no safety requirements.
    unsafe impl NSObjectProtocol for NativePopoverTarget {}

    impl NativePopoverTarget {
        #[unsafe(method(startFixture:))]
        fn start_fixture(&self, _sender: Option<&AnyObject>) {
            self.apply_action(MenuAction::Start);
        }

        #[unsafe(method(pauseFixture:))]
        fn pause_fixture(&self, _sender: Option<&AnyObject>) {
            self.apply_action(MenuAction::Pause);
        }

        #[unsafe(method(resumeFixture:))]
        fn resume_fixture(&self, _sender: Option<&AnyObject>) {
            self.apply_action(MenuAction::Resume);
        }

        #[unsafe(method(retryFixture:))]
        fn retry_fixture(&self, _sender: Option<&AnyObject>) {
            self.apply_action(MenuAction::Retry);
        }

        #[unsafe(method(showCancelConfirmation:))]
        fn show_cancel_confirmation(&self, _sender: Option<&AnyObject>) {
            self.ivars().state.borrow_mut().show_cancel_confirmation();
        }

        #[unsafe(method(keepPartialFixture:))]
        fn keep_partial_fixture(&self, _sender: Option<&AnyObject>) {
            self.ivars().state.borrow_mut().keep_partial_fixture();
        }

        #[unsafe(method(discardPartialFixture:))]
        fn discard_partial_fixture(&self, _sender: Option<&AnyObject>) {
            let mtm = MainThreadMarker::new()
                .expect("AppKit must send popover actions on the main thread");
            let mut state = self.ivars().state.borrow_mut();
            if state.discard_partial_fixture() {
                state.render(self, action_selectors(), mtm);
            }
        }

        #[unsafe(method(quit:))]
        fn quit(&self, _sender: Option<&AnyObject>) {
            self.ivars().app_handle.exit(0);
        }
    }
);

impl NativePopoverTarget {
    fn new(
        app_handle: AppHandle,
        state: Rc<RefCell<NativePopoverState>>,
        mtm: MainThreadMarker,
    ) -> Retained<Self> {
        let this = Self::alloc(mtm).set_ivars(NativePopoverTargetIvars { app_handle, state });

        // SAFETY: NSObject's init selector has the expected signature.
        unsafe { msg_send![super(this), init] }
    }

    fn apply_action(&self, action: MenuAction) {
        let mtm =
            MainThreadMarker::new().expect("AppKit must send popover actions on the main thread");
        let mut state = self.ivars().state.borrow_mut();
        if state.apply_fixture_action(action) {
            state.render(self, action_selectors(), mtm);
        }
    }

    fn toggle(&self, mtm: MainThreadMarker) {
        // AppKit may invoke popover delegates synchronously. Clone the native
        // handles before opening or closing so those callbacks never re-enter
        // this RefCell while it is borrowed.
        let (popover, status_item) = {
            let state = self.ivars().state.borrow();
            (state.popover.clone(), state.status_item.clone())
        };

        if popover.isShown() {
            popover.close();
            return;
        }

        let button = status_item
            .button(mtm)
            .expect("the macOS status item must expose its button");
        popover.showRelativeToRect_ofView_preferredEdge(button.bounds(), &button, NSRectEdge::MinY);
    }
}

pub(crate) struct NativePopoverController {
    status_item: Retained<NSStatusItem>,
    _popover: Retained<NSPopover>,
    _content_view_controller: Retained<NSViewController>,
    _delegate: Retained<NativePopoverDelegate>,
    _target: Retained<NativePopoverTarget>,
}

impl NativePopoverController {
    pub(crate) fn attach(
        status_item: Retained<NSStatusItem>,
        app_handle: AppHandle,
        mtm: MainThreadMarker,
    ) -> Self {
        status_item.setMenu(None);
        let button = status_item
            .button(mtm)
            .expect("the macOS status item must expose its button");
        let loxa = NSString::from_str("Loxa");
        button.setAlphaValue(1.0);
        button.setToolTip(Some(&loxa));
        button.setAccessibilityLabel(Some(&loxa));

        let popover = NSPopover::new(mtm);
        popover.setBehavior(NSPopoverBehavior::Transient);
        let content_view_controller = NSViewController::new(mtm);
        popover.setContentViewController(Some(&content_view_controller));

        let fixture = selected_fixture();
        let state = Rc::new(RefCell::new(NativePopoverState::new(
            status_item.clone(),
            popover.clone(),
            content_view_controller.clone(),
            fixture,
        )));
        let target = NativePopoverTarget::new(app_handle, state.clone(), mtm);
        state.borrow_mut().render(&target, action_selectors(), mtm);

        let delegate = NativePopoverDelegate::new(state, mtm);
        popover.setDelegate(Some(ProtocolObject::from_ref(&*delegate)));

        Self {
            status_item,
            _popover: popover,
            _content_view_controller: content_view_controller,
            _delegate: delegate,
            _target: target,
        }
    }

    pub(crate) fn toggle(&self, mtm: MainThreadMarker) {
        self._target.toggle(mtm);
    }
}

impl Drop for NativePopoverController {
    fn drop(&mut self) {
        self.status_item.setMenu(None);
    }
}

fn action_selectors() -> Actions {
    Actions {
        start: sel!(startFixture:),
        pause: sel!(pauseFixture:),
        resume: sel!(resumeFixture:),
        retry: sel!(retryFixture:),
        cancel: sel!(showCancelConfirmation:),
        keep_partial: sel!(keepPartialFixture:),
        discard_partial: sel!(discardPartialFixture:),
        quit: sel!(quit:),
    }
}

#[cfg(debug_assertions)]
fn selected_fixture() -> Fixture {
    std::env::var("LOXA_MENU_FIXTURE")
        .ok()
        .as_deref()
        .and_then(Fixture::parse)
        .unwrap_or(Fixture::Empty)
}

#[cfg(not(debug_assertions))]
fn selected_fixture() -> Fixture {
    Fixture::Empty
}
