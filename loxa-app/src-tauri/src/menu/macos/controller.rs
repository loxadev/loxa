#![cfg_attr(all(debug_assertions, not(test)), allow(dead_code))]

use std::cell::RefCell;
use std::rc::Rc;
#[cfg(not(any(test, debug_assertions)))]
use std::time::Instant;

use objc2::rc::Retained;
use objc2::runtime::{AnyObject, ProtocolObject, Sel};
use objc2::{define_class, msg_send, sel, DefinedClass, MainThreadMarker, MainThreadOnly};
use objc2_app_kit::{
    NSAccessibility, NSPopover, NSPopoverBehavior, NSPopoverDelegate, NSStatusItem,
    NSViewController,
};
use objc2_foundation::{NSNotification, NSObject, NSObjectProtocol, NSRectEdge, NSString};
use tauri::AppHandle;

use super::rows::{Actions, MenuRows, PopoverContent};
#[cfg(not(any(test, debug_assertions)))]
use super::timer::{weak_callback, ObservationTimer};
#[cfg(not(any(test, debug_assertions)))]
use crate::menu::observation::{ObservationClient, ObservationMessage};
#[cfg(any(test, debug_assertions))]
use crate::menu::presentation::{Fixture, InlineCancelState, MenuAction};
use crate::menu::presentation::{MenuSnapshot, MenuUpdate};

struct NativePopoverState {
    status_item: Retained<NSStatusItem>,
    popover: Retained<NSPopover>,
    content_view_controller: Retained<NSViewController>,
    snapshot: MenuSnapshot,
    rendered: Option<MenuSnapshot>,
    #[cfg(any(test, debug_assertions))]
    cancel: InlineCancelState,
    #[cfg(not(any(test, debug_assertions)))]
    observation: ObservationClient,
    rows: Option<MenuRows>,
}

impl NativePopoverState {
    fn new(
        status_item: Retained<NSStatusItem>,
        popover: Retained<NSPopover>,
        content_view_controller: Retained<NSViewController>,
        #[cfg(any(test, debug_assertions))] fixture: Fixture,
    ) -> Self {
        #[cfg(any(test, debug_assertions))]
        let snapshot = fixture.snapshot();
        #[cfg(not(any(test, debug_assertions)))]
        let snapshot = MenuSnapshot::loading();
        Self {
            status_item,
            popover,
            content_view_controller,
            snapshot,
            rendered: None,
            #[cfg(any(test, debug_assertions))]
            cancel: InlineCancelState::default(),
            #[cfg(not(any(test, debug_assertions)))]
            observation: ObservationClient::start(),
            rows: None,
        }
    }

    fn render(&mut self, target: &AnyObject, actions: Actions, mtm: MainThreadMarker) {
        match self.snapshot.update_from(self.rendered.as_ref()) {
            MenuUpdate::Rebuild => self.rebuild(target, actions, mtm),
            MenuUpdate::UpdateRetainedRows => {
                if let Some(rows) = &mut self.rows {
                    #[cfg(any(test, debug_assertions))]
                    rows.update(&self.snapshot, &self.cancel);
                    #[cfg(not(any(test, debug_assertions)))]
                    rows.update(&self.snapshot);
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
        #[cfg(any(test, debug_assertions))]
        {
            self.cancel.reset();
            if let Some(rows) = &mut self.rows {
                rows.update_cancel_controls(&self.cancel);
            }
        }
    }

    #[cfg(any(test, debug_assertions))]
    fn apply_fixture_action(&mut self, action: MenuAction) -> bool {
        let Some(snapshot) = self.snapshot.apply_fixture_action(action) else {
            return false;
        };
        self.snapshot = snapshot;
        self.cancel.reset();
        true
    }

    #[cfg(any(test, debug_assertions))]
    fn show_cancel_confirmation(&mut self) {
        if let Some(rows) = &mut self.rows {
            rows.show_cancel_confirmation(&mut self.cancel);
        } else {
            self.cancel.activate_cancel();
        }
    }

    #[cfg(any(test, debug_assertions))]
    fn keep_partial_fixture(&mut self) {
        self.cancel.keep_partial();
        self.update_cancel_controls();
    }

    #[cfg(any(test, debug_assertions))]
    fn discard_partial_fixture(&mut self) -> bool {
        if !self.cancel.discard_partial() {
            return false;
        }

        // This only changes the injected fixture displayed by the native popover.
        // It never touches a managed artifact or starts a background operation.
        self.snapshot = Fixture::Empty.snapshot();
        true
    }

    #[cfg(any(test, debug_assertions))]
    fn update_cancel_controls(&mut self) {
        if let Some(rows) = &mut self.rows {
            rows.update_cancel_controls(&self.cancel);
        }
    }

    fn popover_opened(&mut self) {
        #[cfg(not(any(test, debug_assertions)))]
        self.observation.request_popover_open(Instant::now());
    }

    #[cfg(not(any(test, debug_assertions)))]
    fn drain_observations(&mut self, target: &AnyObject, actions: Actions, mtm: MainThreadMarker) {
        let Some(message) = self.observation.drain(Instant::now()) else {
            return;
        };
        self.snapshot = match message {
            ObservationMessage::Snapshot(snapshot) => snapshot,
            ObservationMessage::Error(error) => MenuSnapshot::error(error),
        };
        self.render(target, actions, mtm);
    }

    fn shutdown(&mut self) {
        #[cfg(not(any(test, debug_assertions)))]
        self.observation.shutdown();
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
        #[cfg(any(test, debug_assertions))]
        #[unsafe(method(startFixture:))]
        fn start_fixture(&self, _sender: Option<&AnyObject>) {
            self.apply_action(MenuAction::Start);
        }

        #[cfg(any(test, debug_assertions))]
        #[unsafe(method(pauseFixture:))]
        fn pause_fixture(&self, _sender: Option<&AnyObject>) {
            self.apply_action(MenuAction::Pause);
        }

        #[cfg(any(test, debug_assertions))]
        #[unsafe(method(resumeFixture:))]
        fn resume_fixture(&self, _sender: Option<&AnyObject>) {
            self.apply_action(MenuAction::Resume);
        }

        #[cfg(any(test, debug_assertions))]
        #[unsafe(method(retryFixture:))]
        fn retry_fixture(&self, _sender: Option<&AnyObject>) {
            self.apply_action(MenuAction::Retry);
        }

        #[cfg(any(test, debug_assertions))]
        #[unsafe(method(showCancelConfirmation:))]
        fn show_cancel_confirmation(&self, _sender: Option<&AnyObject>) {
            self.ivars().state.borrow_mut().show_cancel_confirmation();
        }

        #[cfg(any(test, debug_assertions))]
        #[unsafe(method(keepPartialFixture:))]
        fn keep_partial_fixture(&self, _sender: Option<&AnyObject>) {
            self.ivars().state.borrow_mut().keep_partial_fixture();
        }

        #[cfg(any(test, debug_assertions))]
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
            crate::app::request_native_shell_exit(&self.ivars().app_handle);
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

    #[cfg(any(test, debug_assertions))]
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

        self.ivars().state.borrow_mut().popover_opened();

        let button = status_item
            .button(mtm)
            .expect("the macOS status item must expose its button");
        popover.showRelativeToRect_ofView_preferredEdge(button.bounds(), &button, NSRectEdge::MinY);
    }

    #[cfg(not(any(test, debug_assertions)))]
    fn drain_observations(&self) {
        let mtm =
            MainThreadMarker::new().expect("AppKit must drain observations on the main thread");
        self.ivars()
            .state
            .borrow_mut()
            .drain_observations(self, action_selectors(), mtm);
    }

    fn shutdown(&self) {
        self.ivars().state.borrow_mut().shutdown();
    }
}

pub(crate) struct NativePopoverController {
    status_item: Retained<NSStatusItem>,
    _popover: Retained<NSPopover>,
    _content_view_controller: Retained<NSViewController>,
    _delegate: Retained<NativePopoverDelegate>,
    _target: Retained<NativePopoverTarget>,
    #[cfg(not(any(test, debug_assertions)))]
    timer: ObservationTimer,
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

        #[cfg(any(test, debug_assertions))]
        let fixture = selected_fixture();
        let state = Rc::new(RefCell::new(NativePopoverState::new(
            status_item.clone(),
            popover.clone(),
            content_view_controller.clone(),
            #[cfg(any(test, debug_assertions))]
            fixture,
        )));
        let target = NativePopoverTarget::new(app_handle, state.clone(), mtm);
        state.borrow_mut().render(&target, action_selectors(), mtm);

        let delegate = NativePopoverDelegate::new(state, mtm);
        popover.setDelegate(Some(ProtocolObject::from_ref(&*delegate)));

        #[cfg(not(any(test, debug_assertions)))]
        let timer = ObservationTimer::schedule(
            0.25,
            weak_callback(&target, NativePopoverTarget::drain_observations),
            mtm,
        );

        Self {
            status_item,
            _popover: popover,
            _content_view_controller: content_view_controller,
            _delegate: delegate,
            _target: target,
            #[cfg(not(any(test, debug_assertions)))]
            timer,
        }
    }

    pub(crate) fn toggle(&self, mtm: MainThreadMarker) {
        self._target.toggle(mtm);
    }
}

impl Drop for NativePopoverController {
    fn drop(&mut self) {
        #[cfg(not(any(test, debug_assertions)))]
        self.timer.shutdown();
        self._target.shutdown();
        self.status_item.setMenu(None);
    }
}

struct ProductionActionSelectors {
    quit: Sel,
}

fn production_action_selectors() -> ProductionActionSelectors {
    ProductionActionSelectors { quit: sel!(quit:) }
}

fn action_selectors() -> Actions {
    let ProductionActionSelectors { quit } = production_action_selectors();
    Actions {
        #[cfg(any(test, debug_assertions))]
        start: sel!(startFixture:),
        #[cfg(any(test, debug_assertions))]
        pause: sel!(pauseFixture:),
        #[cfg(any(test, debug_assertions))]
        resume: sel!(resumeFixture:),
        #[cfg(any(test, debug_assertions))]
        retry: sel!(retryFixture:),
        #[cfg(any(test, debug_assertions))]
        cancel: sel!(showCancelConfirmation:),
        #[cfg(any(test, debug_assertions))]
        keep_partial: sel!(keepPartialFixture:),
        #[cfg(any(test, debug_assertions))]
        discard_partial: sel!(discardPartialFixture:),
        quit,
    }
}

#[cfg(any(test, debug_assertions))]
fn selected_fixture() -> Fixture {
    std::env::var("LOXA_MENU_FIXTURE")
        .ok()
        .as_deref()
        .and_then(Fixture::parse)
        .unwrap_or(Fixture::Empty)
}

#[cfg(test)]
mod tests {
    use objc2::sel;

    use super::{production_action_selectors, ProductionActionSelectors};

    #[test]
    fn production_actions_expose_quit_without_fixture_selectors() {
        let ProductionActionSelectors { quit } = production_action_selectors();

        assert_eq!(quit, sel!(quit:));
    }
}
