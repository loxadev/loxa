use std::cell::RefCell;
use std::rc::Rc;
use std::time::Instant;

use objc2::rc::{Retained, Weak};
use objc2::runtime::{AnyObject, ProtocolObject, Sel};
use objc2::{define_class, msg_send, sel, DefinedClass, MainThreadMarker, MainThreadOnly};
use objc2_app_kit::{
    NSAccessibility, NSButton, NSPasteboard, NSPasteboardTypeString, NSPopover, NSPopoverBehavior,
    NSPopoverDelegate, NSSearchField, NSStatusItem, NSViewController,
};
use objc2_foundation::{NSNotification, NSObject, NSObjectProtocol, NSRectEdge, NSString};
use tauri::AppHandle;

use super::installed_rows::{self, InstalledAction};
use super::rows::{Actions, MenuRows, PopoverContent};
#[cfg(not(test))]
use super::timer::{weak_callback, ObservationTimer};
use crate::menu::catalog::{CatalogEvent, CatalogState};
use crate::menu::incomplete::{DiscardFailure, IncompleteState};
use crate::menu::installed::InstalledState;
use crate::menu::observation::BackendClient;
#[cfg(not(test))]
use crate::menu::observation::{BackendMessage, ObservationMessage};
#[cfg(test)]
use crate::menu::presentation::{Fixture, InlineCancelState, MenuAction};
use crate::menu::presentation::{MenuSnapshot, MenuUpdate};

struct NativePopoverState {
    status_item: Retained<NSStatusItem>,
    popover: Retained<NSPopover>,
    content_view_controller: Retained<NSViewController>,
    snapshot: MenuSnapshot,
    rendered: Option<MenuSnapshot>,
    catalog: CatalogState,
    rendered_catalog: Option<CatalogState>,
    installed: Rc<RefCell<InstalledState>>,
    rendered_installed: Option<InstalledState>,
    incomplete: Rc<RefCell<IncompleteState>>,
    rendered_incomplete: Option<IncompleteState>,
    #[cfg(test)]
    cancel: InlineCancelState,
    backend: Option<BackendClient>,
    rows: Option<MenuRows>,
}

impl NativePopoverState {
    fn new(
        status_item: Retained<NSStatusItem>,
        popover: Retained<NSPopover>,
        content_view_controller: Retained<NSViewController>,
        #[cfg(test)] fixture: Fixture,
    ) -> Self {
        #[cfg(test)]
        let snapshot = fixture.snapshot();
        #[cfg(not(test))]
        let snapshot = MenuSnapshot::loading();
        Self {
            status_item,
            popover,
            content_view_controller,
            snapshot,
            rendered: None,
            catalog: CatalogState::default(),
            rendered_catalog: None,
            installed: Rc::new(RefCell::new(InstalledState::default())),
            rendered_installed: None,
            incomplete: Rc::new(RefCell::new(IncompleteState::default())),
            rendered_incomplete: None,
            #[cfg(test)]
            cancel: InlineCancelState::default(),
            backend: {
                #[cfg(test)]
                {
                    None
                }
                #[cfg(not(test))]
                {
                    Some(BackendClient::start())
                }
            },
            rows: None,
        }
    }

    fn render(&mut self, target: &AnyObject, actions: Actions, mtm: MainThreadMarker) {
        let catalog_changed = self.rendered_catalog.as_ref() != Some(&self.catalog);
        let installed_changed = self.rendered_installed.as_ref() != Some(&*self.installed.borrow());
        let incomplete_changed =
            self.rendered_incomplete.as_ref() != Some(&*self.incomplete.borrow());
        let snapshot_update = self.snapshot.update_from(self.rendered.as_ref());
        let catalog_updated_in_place = catalog_changed
            && !installed_changed
            && !incomplete_changed
            && snapshot_update == MenuUpdate::UpdateRetainedRows
            && self.rows.as_ref().is_some_and(|rows| {
                self.rendered_catalog
                    .as_ref()
                    .is_some_and(|previous| rows.update_catalog_transfer(previous, &self.catalog))
            });
        let requires_rebuild = installed_changed
            || incomplete_changed
            || snapshot_update == MenuUpdate::Rebuild
            || (catalog_changed && !catalog_updated_in_place)
            || self.rows.is_none();
        if requires_rebuild {
            self.rebuild(target, actions, mtm);
        } else if let Some(rows) = &mut self.rows {
            #[cfg(test)]
            rows.update(&self.snapshot, &self.cancel);
            #[cfg(not(test))]
            rows.update(&self.snapshot);
        }
        self.rendered = Some(self.snapshot.clone());
        self.rendered_catalog = Some(self.catalog.clone());
        self.rendered_installed = Some(self.installed.borrow().clone());
        self.rendered_incomplete = Some(self.incomplete.borrow().clone());
    }

    fn rebuild(&mut self, target: &AnyObject, actions: Actions, mtm: MainThreadMarker) {
        let search_focus = self.rows.as_ref().and_then(MenuRows::capture_search_focus);
        let PopoverContent { view, rows, .. } = {
            let incomplete = self.incomplete.borrow();
            let installed = self.installed.borrow();
            MenuRows::build(
                &self.snapshot,
                &self.catalog,
                &incomplete,
                &installed,
                Some(target),
                actions,
                mtm,
            )
        };
        if let Some(rows) = &self.rows {
            rows.prepare_for_replacement();
        }
        self.popover.setContentSize(view.frame().size);
        self.content_view_controller.setView(&view);
        if let Some(search_focus) = search_focus {
            rows.restore_search_focus(search_focus);
        }
        self.rows = Some(rows);
    }

    fn popover_closed(&mut self, target: &AnyObject, actions: Actions, mtm: MainThreadMarker) {
        self.installed.borrow_mut().reset_feedback();
        self.rendered_installed = None;
        self.cancel_incomplete_discard();
        self.rendered_incomplete = None;
        #[cfg(test)]
        {
            self.cancel.reset();
        }
        self.render(target, actions, mtm);
    }

    fn cancel_incomplete_discard(&mut self) {
        let model_id = self.incomplete.borrow_mut().cancel_prepared();
        if let (Some(model_id), Some(backend)) = (model_id, self.backend.as_mut()) {
            let _ = backend.keep_discard(model_id);
        }
    }

    #[cfg(test)]
    fn apply_fixture_action(&mut self, action: MenuAction) -> bool {
        let Some(snapshot) = self.snapshot.apply_fixture_action(action) else {
            return false;
        };
        self.snapshot = snapshot;
        self.cancel.reset();
        true
    }

    #[cfg(test)]
    fn show_cancel_confirmation(&mut self) {
        if let Some(rows) = &mut self.rows {
            rows.show_cancel_confirmation(&mut self.cancel);
        } else {
            self.cancel.activate_cancel();
        }
    }

    #[cfg(test)]
    fn keep_partial_fixture(&mut self) {
        self.cancel.keep_partial();
        self.update_cancel_controls();
    }

    #[cfg(test)]
    fn discard_partial_fixture(&mut self) -> bool {
        if !self.cancel.discard_partial() {
            return false;
        }

        // This only changes the injected fixture displayed by the native popover.
        // It never touches a managed artifact or starts a background operation.
        self.snapshot = Fixture::Empty.snapshot();
        true
    }

    #[cfg(test)]
    fn update_cancel_controls(&mut self) {
        if let Some(rows) = &mut self.rows {
            rows.update_cancel_controls(&self.cancel);
        }
    }

    fn popover_opened(&mut self) {
        if let Some(backend) = &mut self.backend {
            backend.request_popover_open(Instant::now());
        }
    }

    #[cfg(not(test))]
    fn drain_backend(&mut self, target: &AnyObject, actions: Actions, mtm: MainThreadMarker) {
        let Some(backend) = &mut self.backend else {
            return;
        };
        let messages = backend.drain(Instant::now());
        if messages.is_empty() {
            return;
        }
        for message in messages {
            match message {
                BackendMessage::Observation(ObservationMessage::Snapshot(snapshot)) => {
                    self.snapshot = snapshot;
                }
                BackendMessage::Observation(ObservationMessage::Error(error)) => {
                    self.snapshot = MenuSnapshot::error(error.clone());
                    let _ = self.catalog.apply(CatalogEvent::Failed {
                        generation: self.catalog.generation(),
                        message: error,
                    });
                }
                BackendMessage::Installed {
                    result,
                    pinned_model_id,
                } => match result {
                    Ok(items) => self.installed.borrow_mut().replace(items, pinned_model_id),
                    Err(error) => self.installed.borrow_mut().fail(error),
                },
                BackendMessage::Incomplete(result) => match result {
                    Ok(items) => self.incomplete.borrow_mut().replace(items),
                    Err(error) => self.incomplete.borrow_mut().fail(error),
                },
                BackendMessage::DiscardPrepared { model_id, result } => {
                    let _ = self.incomplete.borrow_mut().prepared(&model_id, result);
                }
                BackendMessage::DiscardCompleted { model_id, result } => {
                    let _ = self.incomplete.borrow_mut().completed(&model_id, result);
                }
                BackendMessage::Catalog(event) => {
                    let _ = self.catalog.apply(event);
                }
            }
        }
        self.render(target, actions, mtm);
    }

    fn shutdown(&mut self) {
        self.cancel_incomplete_discard();
        if let Some(backend) = &mut self.backend {
            backend.shutdown();
        }
    }

    fn dispatch_catalog(&mut self, command: Option<crate::menu::catalog::CatalogCommand>) {
        let Some(command) = command else {
            return;
        };
        let generation = command.generation();
        if !self
            .backend
            .as_mut()
            .is_some_and(|backend| backend.dispatch(command))
        {
            let _ = self.catalog.apply(CatalogEvent::Failed {
                generation,
                message: "The menu backend is unavailable".into(),
            });
        }
    }
}

fn copy_runtime_curl_with<Copy>(snapshot: &MenuSnapshot, copy: Copy) -> bool
where
    Copy: FnOnce(&str) -> bool,
{
    snapshot
        .runtime_curl_command()
        .is_some_and(|command| copy(&command))
}

fn copy_runtime_curl_to_pasteboard(snapshot: &MenuSnapshot) -> bool {
    copy_runtime_curl_with(snapshot, |command| {
        let pasteboard = NSPasteboard::generalPasteboard();
        pasteboard.clearContents();
        // SAFETY: AppKit initializes this immutable standard pasteboard type.
        let string_type = unsafe { NSPasteboardTypeString };
        pasteboard.setString_forType(&NSString::from_str(command), string_type)
    })
}

struct NativePopoverDelegateIvars {
    target: Weak<NativePopoverTarget>,
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
            let Some(target) = self.ivars().target.load() else {
                return;
            };
            let mtm = MainThreadMarker::new()
                .expect("AppKit must close the native popover on the main thread");
            target.popover_closed(mtm);
        }
    }
);

impl NativePopoverDelegate {
    fn new(target: &Retained<NativePopoverTarget>, mtm: MainThreadMarker) -> Retained<Self> {
        let this = Self::alloc(mtm).set_ivars(NativePopoverDelegateIvars {
            target: Weak::from_retained(target),
        });

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
        #[unsafe(method(copyRuntimeCurl:))]
        fn copy_runtime_curl(&self, _sender: Option<&NSButton>) {
            let snapshot = self.ivars().state.borrow().snapshot.clone();
            let _ = copy_runtime_curl_to_pasteboard(&snapshot);
        }

        #[unsafe(method(submitSearch:))]
        fn submit_search(&self, sender: Option<&NSSearchField>) {
            let query = sender
                .map(|field| field.stringValue().to_string())
                .unwrap_or_default();
            let mtm = MainThreadMarker::new()
                .expect("AppKit must submit menu searches on the main thread");
            let mut state = self.ivars().state.borrow_mut();
            state.cancel_incomplete_discard();
            let command = state.catalog.submit_search(&query);
            state.dispatch_catalog(command);
            state.render(self, action_selectors(), mtm);
        }

        #[unsafe(method(inspectRepository:))]
        fn inspect_repository(&self, sender: Option<&NSButton>) {
            let Some(index) = sender.and_then(|button| usize::try_from(button.tag()).ok()) else {
                return;
            };
            let mtm = MainThreadMarker::new()
                .expect("AppKit must select repositories on the main thread");
            let mut state = self.ivars().state.borrow_mut();
            state.cancel_incomplete_discard();
            let command = state.catalog.inspect_repository(index);
            state.dispatch_catalog(command);
            state.render(self, action_selectors(), mtm);
        }

        #[unsafe(method(selectCandidate:))]
        fn select_candidate(&self, sender: Option<&NSButton>) {
            let Some(index) = sender.and_then(|button| usize::try_from(button.tag()).ok()) else {
                return;
            };
            let mtm = MainThreadMarker::new()
                .expect("AppKit must select GGUF candidates on the main thread");
            let mut state = self.ivars().state.borrow_mut();
            state.cancel_incomplete_discard();
            if state.catalog.select_candidate(index) {
                state.render(self, action_selectors(), mtm);
            }
        }

        #[unsafe(method(transferSelected:))]
        fn transfer_selected(&self, _sender: Option<&NSButton>) {
            let mtm = MainThreadMarker::new()
                .expect("AppKit must start transfers on the main thread");
            let mut state = self.ivars().state.borrow_mut();
            state.cancel_incomplete_discard();
            let command = state.catalog.start_transfer();
            state.dispatch_catalog(command);
            state.render(self, action_selectors(), mtm);
        }

        #[unsafe(method(pauseTransfer:))]
        fn pause_transfer(&self, _sender: Option<&NSButton>) {
            let mtm = MainThreadMarker::new()
                .expect("AppKit must pause transfers on the main thread");
            let mut state = self.ivars().state.borrow_mut();
            let generation = state.catalog.generation();
            let requested = state
                .backend
                .as_ref()
                .is_some_and(|backend| backend.request_pause(generation));
            if requested && state.catalog.request_pause() {
                state.render(self, action_selectors(), mtm);
            }
        }

        #[unsafe(method(selectInstalled:))]
        fn select_installed(&self, sender: Option<&NSButton>) {
            let Some(index) = sender.and_then(|button| usize::try_from(button.tag()).ok()) else {
                return;
            };
            let installed = self.ivars().state.borrow().installed.clone();
            let model_id = installed
                .borrow()
                .visible_items()
                .get(index)
                .map(|item| item.id().to_owned());
            let Some(model_id) = model_id else {
                return;
            };
            if !installed.borrow_mut().select(&model_id) {
                return;
            }
            let mtm = MainThreadMarker::new()
                .expect("AppKit must select installed models on the main thread");
            let mut state = self.ivars().state.borrow_mut();
            state.cancel_incomplete_discard();
            state.render(self, action_selectors(), mtm);
        }

        #[unsafe(method(prepareIncompleteDiscard:))]
        fn prepare_incomplete_discard(&self, sender: Option<&NSButton>) {
            let Some(index) = sender.and_then(|button| usize::try_from(button.tag()).ok()) else {
                return;
            };
            let mtm = MainThreadMarker::new()
                .expect("AppKit must prepare incomplete discard on the main thread");
            let mut state = self.ivars().state.borrow_mut();
            let Some(model_id) = state.incomplete.borrow_mut().prepare_discard(index) else {
                return;
            };
            if !state
                .backend
                .as_mut()
                .is_some_and(|backend| backend.prepare_discard(model_id.clone()))
            {
                let _ = state
                    .incomplete
                    .borrow_mut()
                    .prepared(&model_id, Err(DiscardFailure::Unavailable));
            }
            state.render(self, action_selectors(), mtm);
        }

        #[unsafe(method(keepIncompletePartial:))]
        fn keep_incomplete_partial(&self, _sender: Option<&NSButton>) {
            let mtm = MainThreadMarker::new()
                .expect("AppKit must keep incomplete downloads on the main thread");
            let mut state = self.ivars().state.borrow_mut();
            let model_id = { state.incomplete.borrow_mut().keep_partial() };
            if let Some(model_id) = model_id {
                if let Some(backend) = state.backend.as_mut() {
                    let _ = backend.keep_discard(model_id);
                }
            }
            state.render(self, action_selectors(), mtm);
        }

        #[unsafe(method(confirmIncompleteDiscard:))]
        fn confirm_incomplete_discard(&self, _sender: Option<&NSButton>) {
            let mtm = MainThreadMarker::new()
                .expect("AppKit must confirm incomplete discard on the main thread");
            let mut state = self.ivars().state.borrow_mut();
            let Some(model_id) = state.incomplete.borrow_mut().confirm_discard() else {
                return;
            };
            if !state
                .backend
                .as_mut()
                .is_some_and(|backend| backend.confirm_discard(model_id.clone()))
            {
                let _ = state
                    .incomplete
                    .borrow_mut()
                    .completed(&model_id, Err(DiscardFailure::Unavailable));
            }
            state.render(self, action_selectors(), mtm);
        }

        #[unsafe(method(copyInstalledCommand:))]
        fn copy_installed_command(&self, _sender: Option<&NSButton>) {
            self.perform_installed_action(InstalledAction::CopyChatCommand);
        }

        #[unsafe(method(revealInstalled:))]
        fn reveal_installed(&self, _sender: Option<&NSButton>) {
            self.perform_installed_action(InstalledAction::RevealInFinder);
        }

        #[cfg(test)]
        #[unsafe(method(startFixture:))]
        fn start_fixture(&self, _sender: Option<&AnyObject>) {
            self.apply_action(MenuAction::Start);
        }

        #[cfg(test)]
        #[unsafe(method(pauseFixture:))]
        fn pause_fixture(&self, _sender: Option<&AnyObject>) {
            self.apply_action(MenuAction::Pause);
        }

        #[cfg(test)]
        #[unsafe(method(resumeFixture:))]
        fn resume_fixture(&self, _sender: Option<&AnyObject>) {
            self.apply_action(MenuAction::Resume);
        }

        #[cfg(test)]
        #[unsafe(method(retryFixture:))]
        fn retry_fixture(&self, _sender: Option<&AnyObject>) {
            self.apply_action(MenuAction::Retry);
        }

        #[cfg(test)]
        #[unsafe(method(showCancelConfirmation:))]
        fn show_cancel_confirmation(&self, _sender: Option<&AnyObject>) {
            self.ivars().state.borrow_mut().show_cancel_confirmation();
        }

        #[cfg(test)]
        #[unsafe(method(keepPartialFixture:))]
        fn keep_partial_fixture(&self, _sender: Option<&AnyObject>) {
            self.ivars().state.borrow_mut().keep_partial_fixture();
        }

        #[cfg(test)]
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

    #[cfg(test)]
    fn apply_action(&self, action: MenuAction) {
        let mtm =
            MainThreadMarker::new().expect("AppKit must send popover actions on the main thread");
        let mut state = self.ivars().state.borrow_mut();
        if state.apply_fixture_action(action) {
            state.render(self, action_selectors(), mtm);
        }
    }

    fn perform_installed_action(&self, action: InstalledAction) {
        let installed = self.ivars().state.borrow().installed.clone();
        installed_rows::dispatch_native_selected_action(&installed, action);
        let mtm = MainThreadMarker::new()
            .expect("AppKit must perform installed actions on the main thread");
        self.ivars()
            .state
            .borrow_mut()
            .render(self, action_selectors(), mtm);
    }

    fn popover_closed(&self, mtm: MainThreadMarker) {
        self.ivars()
            .state
            .borrow_mut()
            .popover_closed(self, action_selectors(), mtm);
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

    #[cfg(not(test))]
    fn drain_backend(&self) {
        let mtm =
            MainThreadMarker::new().expect("AppKit must drain backend events on the main thread");
        self.ivars()
            .state
            .borrow_mut()
            .drain_backend(self, action_selectors(), mtm);
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
    #[cfg(not(test))]
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

        #[cfg(test)]
        let fixture = selected_fixture();
        let state = Rc::new(RefCell::new(NativePopoverState::new(
            status_item.clone(),
            popover.clone(),
            content_view_controller.clone(),
            #[cfg(test)]
            fixture,
        )));
        let target = NativePopoverTarget::new(app_handle, state.clone(), mtm);
        state.borrow_mut().render(&target, action_selectors(), mtm);

        let delegate = NativePopoverDelegate::new(&target, mtm);
        popover.setDelegate(Some(ProtocolObject::from_ref(&*delegate)));

        #[cfg(not(test))]
        let timer = ObservationTimer::schedule(
            0.25,
            weak_callback(&target, NativePopoverTarget::drain_backend),
            mtm,
        );

        Self {
            status_item,
            _popover: popover,
            _content_view_controller: content_view_controller,
            _delegate: delegate,
            _target: target,
            #[cfg(not(test))]
            timer,
        }
    }

    pub(crate) fn toggle(&self, mtm: MainThreadMarker) {
        self._target.toggle(mtm);
    }
}

impl Drop for NativePopoverController {
    fn drop(&mut self) {
        #[cfg(not(test))]
        self.timer.shutdown();
        self._target.shutdown();
        self.status_item.setMenu(None);
    }
}

struct ProductionActionSelectors {
    runtime_copy: Sel,
    search: Sel,
    repository: Sel,
    candidate: Sel,
    transfer: Sel,
    pause: Sel,
    installed_select: Sel,
    installed_copy: Sel,
    installed_reveal: Sel,
    incomplete_prepare: Sel,
    incomplete_keep: Sel,
    incomplete_confirm: Sel,
    quit: Sel,
}

fn production_action_selectors() -> ProductionActionSelectors {
    ProductionActionSelectors {
        runtime_copy: sel!(copyRuntimeCurl:),
        search: sel!(submitSearch:),
        repository: sel!(inspectRepository:),
        candidate: sel!(selectCandidate:),
        transfer: sel!(transferSelected:),
        pause: sel!(pauseTransfer:),
        installed_select: sel!(selectInstalled:),
        installed_copy: sel!(copyInstalledCommand:),
        installed_reveal: sel!(revealInstalled:),
        incomplete_prepare: sel!(prepareIncompleteDiscard:),
        incomplete_keep: sel!(keepIncompletePartial:),
        incomplete_confirm: sel!(confirmIncompleteDiscard:),
        quit: sel!(quit:),
    }
}

fn action_selectors() -> Actions {
    let ProductionActionSelectors {
        runtime_copy,
        search,
        repository,
        candidate,
        transfer,
        pause,
        installed_select,
        installed_copy,
        installed_reveal,
        incomplete_prepare,
        incomplete_keep,
        incomplete_confirm,
        quit,
    } = production_action_selectors();
    Actions {
        runtime_copy,
        search,
        repository,
        candidate,
        transfer,
        pause_transfer: pause,
        installed_select,
        installed_copy,
        installed_reveal,
        incomplete_prepare,
        incomplete_keep,
        incomplete_confirm,
        #[cfg(test)]
        start: sel!(startFixture:),
        #[cfg(test)]
        pause: sel!(pauseFixture:),
        #[cfg(test)]
        resume: sel!(resumeFixture:),
        #[cfg(test)]
        retry: sel!(retryFixture:),
        #[cfg(test)]
        cancel: sel!(showCancelConfirmation:),
        #[cfg(test)]
        keep_partial: sel!(keepPartialFixture:),
        #[cfg(test)]
        discard_partial: sel!(discardPartialFixture:),
        quit,
    }
}

#[cfg(test)]
fn selected_fixture() -> Fixture {
    std::env::var("LOXA_MENU_FIXTURE")
        .ok()
        .as_deref()
        .and_then(Fixture::parse)
        .unwrap_or(Fixture::Empty)
}

#[cfg(test)]
mod tests {
    use objc2::{sel, ClassType};

    use super::{
        copy_runtime_curl_with, production_action_selectors, NativePopoverTarget,
        ProductionActionSelectors,
    };
    use crate::menu::presentation::Fixture;

    #[test]
    fn runtime_copy_dispatches_the_exact_curl_only_for_a_running_endpoint() {
        let running = Fixture::Running
            .snapshot()
            .with_running_port(43123)
            .unwrap();
        let mut copied = None;
        assert!(copy_runtime_curl_with(&running, |command| {
            copied = Some(command.to_owned());
            true
        }));
        assert_eq!(
            copied.as_deref(),
            Some("curl http://127.0.0.1:43123/v1/models")
        );

        assert!(!copy_runtime_curl_with(
            &Fixture::Installed.snapshot(),
            |_| panic!("idle snapshots must not reach the clipboard")
        ));
    }

    #[test]
    fn production_target_exposes_catalog_and_transfer_actions() {
        let class = NativePopoverTarget::class();

        for action in [
            sel!(copyRuntimeCurl:),
            sel!(submitSearch:),
            sel!(inspectRepository:),
            sel!(selectCandidate:),
            sel!(transferSelected:),
            sel!(pauseTransfer:),
            sel!(selectInstalled:),
            sel!(copyInstalledCommand:),
            sel!(revealInstalled:),
            sel!(prepareIncompleteDiscard:),
            sel!(keepIncompletePartial:),
            sel!(confirmIncompleteDiscard:),
        ] {
            assert!(
                class.instance_method(action).is_some(),
                "production target is missing {action}"
            );
        }
    }

    #[test]
    fn production_actions_expose_catalog_transfer_and_quit_selectors() {
        let ProductionActionSelectors {
            runtime_copy,
            search,
            repository,
            candidate,
            transfer,
            pause,
            installed_select,
            installed_copy,
            installed_reveal,
            incomplete_prepare,
            incomplete_keep,
            incomplete_confirm,
            quit,
        } = production_action_selectors();

        assert_eq!(runtime_copy, sel!(copyRuntimeCurl:));
        assert_eq!(search, sel!(submitSearch:));
        assert_eq!(repository, sel!(inspectRepository:));
        assert_eq!(candidate, sel!(selectCandidate:));
        assert_eq!(transfer, sel!(transferSelected:));
        assert_eq!(pause, sel!(pauseTransfer:));
        assert_eq!(installed_select, sel!(selectInstalled:));
        assert_eq!(installed_copy, sel!(copyInstalledCommand:));
        assert_eq!(installed_reveal, sel!(revealInstalled:));
        assert_eq!(incomplete_prepare, sel!(prepareIncompleteDiscard:));
        assert_eq!(incomplete_keep, sel!(keepIncompletePartial:));
        assert_eq!(incomplete_confirm, sel!(confirmIncompleteDiscard:));
        assert_eq!(quit, sel!(quit:));
    }
}
