use std::cell::RefCell;
use std::rc::Rc;
use std::time::Instant;

use loxa::paths::AppPaths;
use loxa_ipc::ServiceClient;
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
use super::rows::{ActionBindings, Actions, MenuRows, PopoverContent};
#[cfg(not(test))]
use super::timer::{weak_callback, ObservationTimer};
use crate::menu::api_presentation::ApiPresentation;
#[cfg(not(test))]
use crate::menu::api_presentation::ApiPrimaryActionKind;
use crate::menu::api_runtime::ApiRuntimeController;
#[cfg(not(test))]
use crate::menu::catalog::CatalogEvent;
use crate::menu::catalog::CatalogState;
use crate::menu::incomplete::{DiscardFailure, IncompleteState};
use crate::menu::installed::InstalledState;
use crate::menu::observation::BackendClient;
#[cfg(not(test))]
use crate::menu::observation::{BackendMessage, ObservationMessage};
#[cfg(test)]
use crate::menu::presentation::{Fixture, InlineCancelState, MenuAction};
use crate::menu::presentation::{MenuSnapshot, MenuUpdate};

const RUNTIME_CURL_COPY_FEEDBACK_DURATION: std::time::Duration =
    std::time::Duration::from_millis(1_500);

#[derive(Debug, Eq, PartialEq)]
struct RuntimeCurlCopyFeedback {
    command: String,
    expires_at: Instant,
}

pub(crate) struct NativeExitResources {
    runtime: ApiRuntimeController,
    backend: BackendClient,
}

impl NativeExitResources {
    pub(crate) fn shutdown_runtime(&mut self) -> Result<(), ()> {
        self.runtime.shutdown_and_join().map_err(|_| ())
    }

    pub(crate) fn shutdown_backend(&mut self) -> Result<(), ()> {
        if self.backend.shutdown_and_join().is_ok() {
            return Ok(());
        }
        self.runtime.mark_unavailable_after_exit_failure();
        Err(())
    }
}

#[cfg(test)]
pub(crate) fn exit_resources_for_test(
    runtime: ApiRuntimeController,
    backend: BackendClient,
) -> NativeExitResources {
    NativeExitResources { runtime, backend }
}

#[cfg(test)]
pub(crate) fn exit_resources_api(resources: &NativeExitResources) -> ApiPresentation {
    ApiPresentation::from_controller(&resources.runtime)
}

struct NativePopoverState {
    status_item: Retained<NSStatusItem>,
    popover: Retained<NSPopover>,
    content_view_controller: Retained<NSViewController>,
    snapshot: MenuSnapshot,
    rendered: Option<MenuSnapshot>,
    api: ApiPresentation,
    rendered_api: Option<ApiPresentation>,
    api_runtime: Option<ApiRuntimeController>,
    runtime_curl_copy_feedback: Option<RuntimeCurlCopyFeedback>,
    catalog: CatalogState,
    rendered_catalog: Option<CatalogState>,
    installed: Rc<RefCell<InstalledState>>,
    rendered_installed: Option<InstalledState>,
    incomplete: Rc<RefCell<IncompleteState>>,
    rendered_incomplete: Option<IncompleteState>,
    #[cfg(test)]
    cancel: InlineCancelState,
    #[cfg(test)]
    dispatched_catalog: Vec<crate::menu::catalog::CatalogCommand>,
    backend: Option<BackendClient>,
    rows: Option<MenuRows>,
    model_paths: AppPaths,
    shared_service: bool,
}

impl NativePopoverState {
    fn new(
        status_item: Retained<NSStatusItem>,
        popover: Retained<NSPopover>,
        content_view_controller: Retained<NSViewController>,
        backend_paths: AppPaths,
        #[cfg(not(test))] runtime_paths: AppPaths,
        service_client: Option<ServiceClient>,
        #[cfg(test)] fixture: Fixture,
    ) -> Self {
        #[cfg(test)]
        let snapshot = fixture.snapshot();
        #[cfg(not(test))]
        let snapshot = MenuSnapshot::loading();
        #[cfg(test)]
        let api = ApiPresentation::idle();
        #[cfg(not(test))]
        let (api_runtime, api) = {
            let controller = match service_client {
                Some(client) => ApiRuntimeController::start_service(client),
                None => ApiRuntimeController::start(runtime_paths),
            };
            let presentation = project_api_presentation(&controller, &snapshot);
            (controller, presentation)
        };
        let model_paths = backend_paths.clone();
        #[cfg(not(test))]
        let shared_service = api_runtime.is_shared_service();
        #[cfg(test)]
        let shared_service = false;
        #[cfg(test)]
        let _ = service_client;
        Self {
            status_item,
            popover,
            content_view_controller,
            snapshot,
            rendered: None,
            api,
            rendered_api: None,
            api_runtime: {
                #[cfg(test)]
                {
                    None
                }
                #[cfg(not(test))]
                {
                    Some(api_runtime)
                }
            },
            runtime_curl_copy_feedback: None,
            catalog: CatalogState::default(),
            rendered_catalog: None,
            installed: Rc::new(RefCell::new(InstalledState::default())),
            rendered_installed: None,
            incomplete: Rc::new(RefCell::new(IncompleteState::default())),
            rendered_incomplete: None,
            #[cfg(test)]
            cancel: InlineCancelState::default(),
            #[cfg(test)]
            dispatched_catalog: Vec::new(),
            backend: {
                #[cfg(test)]
                {
                    None
                }
                #[cfg(not(test))]
                {
                    Some(BackendClient::start(backend_paths))
                }
            },
            rows: None,
            model_paths,
            shared_service,
        }
    }

    fn render(&mut self, target: &AnyObject, actions: Actions, mtm: MainThreadMarker) {
        self.reconcile_runtime_curl_copy_feedback(Instant::now());
        let catalog_changed = self.rendered_catalog.as_ref() != Some(&self.catalog);
        let installed_changed = self.rendered_installed.as_ref() != Some(&*self.installed.borrow());
        let incomplete_changed =
            self.rendered_incomplete.as_ref() != Some(&*self.incomplete.borrow());
        let snapshot_update = self.snapshot.update_from(self.rendered.as_ref());
        let api_can_update_retained = self
            .rendered_api
            .as_ref()
            .is_some_and(|previous| self.api.can_update_retained_from(previous));
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
            || !api_can_update_retained
            || snapshot_update == MenuUpdate::Rebuild
            || (catalog_changed && !catalog_updated_in_place)
            || self.rows.is_none();
        if requires_rebuild {
            self.rebuild(target, actions, mtm);
        } else if let Some(rows) = &mut self.rows {
            #[cfg(test)]
            rows.update(&self.snapshot, &self.api, &self.cancel);
            #[cfg(not(test))]
            rows.update(&self.snapshot, &self.api);
        }
        if let Some(rows) = &self.rows {
            rows.update_runtime_curl_copy_feedback(self.runtime_curl_copy_feedback.is_some());
        }
        self.rendered = Some(self.snapshot.clone());
        self.rendered_api = Some(self.api.clone());
        self.rendered_catalog = Some(self.catalog.clone());
        self.rendered_installed = Some(self.installed.borrow().clone());
        self.rendered_incomplete = Some(self.incomplete.borrow().clone());
    }

    fn rebuild(&mut self, target: &AnyObject, actions: Actions, mtm: MainThreadMarker) {
        let search_focus = self.rows.as_ref().and_then(MenuRows::capture_search_focus);
        if let Some(query) = search_focus
            .as_ref()
            .map(super::catalog_rows::SearchFocus::normalized_query)
            .filter(|query| *query != self.catalog.query())
        {
            self.cancel_incomplete_discard();
            let command = self.catalog.submit_search(query);
            self.dispatch_catalog(command);
        }
        let PopoverContent { view, rows, .. } = {
            let incomplete = self.incomplete.borrow();
            let installed = self.installed.borrow();
            MenuRows::build(
                &self.snapshot,
                &self.api,
                &self.catalog,
                &incomplete,
                &installed,
                ActionBindings::new(Some(target), actions),
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
        self.runtime_curl_copy_feedback = None;
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
        let _ = request_runtime_probe(self.api_runtime.as_mut());
    }

    fn arm_runtime_curl_copy_feedback(&mut self, command: String, now: Instant) -> bool {
        arm_runtime_curl_copy_feedback(
            &mut self.runtime_curl_copy_feedback,
            &self.api,
            command,
            now,
        )
    }

    fn reconcile_runtime_curl_copy_feedback(&mut self, now: Instant) -> bool {
        reconcile_runtime_curl_copy_feedback(&mut self.runtime_curl_copy_feedback, &self.api, now)
    }

    fn take_exit_resources(&mut self) -> Option<NativeExitResources> {
        if self.api_runtime.is_none() || self.backend.is_none() {
            return None;
        }
        self.cancel_incomplete_discard();
        let runtime = self.api_runtime.as_mut().expect("checked above");
        let _ = runtime.prepare_shutdown();
        self.api = project_api_presentation(runtime, &self.snapshot);
        let backend = self.backend.as_ref().expect("checked above");
        let _ = backend.pause_active_transfer();
        Some(NativeExitResources {
            runtime: self.api_runtime.take().expect("checked above"),
            backend: self.backend.take().expect("checked above"),
        })
    }

    fn restore_exit_resources(&mut self, resources: NativeExitResources) {
        self.api = project_api_presentation(&resources.runtime, &self.snapshot);
        self.api_runtime = Some(resources.runtime);
        self.backend = Some(resources.backend);
    }

    #[cfg(not(test))]
    fn drain_backend(&mut self, target: &AnyObject, actions: Actions, mtm: MainThreadMarker) {
        let now = Instant::now();
        let feedback_changed = self.reconcile_runtime_curl_copy_feedback(now);
        let api_controller_changed =
            drain_runtime_controller(self.api_runtime.as_mut(), &mut self.api);
        let messages = self
            .backend
            .as_mut()
            .map_or_else(Vec::new, |backend| backend.drain(now));
        let backend_changed = !messages.is_empty();
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
        let api_projection_changed = self.api_runtime.as_ref().is_some_and(|controller| {
            let projected = project_api_presentation(controller, &self.snapshot);
            if projected == self.api {
                return false;
            }
            self.api = projected;
            true
        });
        if feedback_changed || api_controller_changed || api_projection_changed || backend_changed {
            self.render(target, actions, mtm);
        }
    }

    fn dispatch_catalog(&mut self, command: Option<crate::menu::catalog::CatalogCommand>) {
        let Some(command) = command else {
            return;
        };
        #[cfg(test)]
        {
            self.dispatched_catalog.push(command);
        }
        #[cfg(not(test))]
        let generation = command.generation();
        #[cfg(not(test))]
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

pub(super) fn request_runtime_probe(controller: Option<&mut ApiRuntimeController>) -> bool {
    controller.is_some_and(ApiRuntimeController::request_probe)
}

pub(super) fn project_api_presentation(
    controller: &ApiRuntimeController,
    snapshot: &MenuSnapshot,
) -> ApiPresentation {
    ApiPresentation::from_controller_with_observed_runtime(controller, snapshot.observed_runtime())
}

pub(super) fn drain_runtime_controller(
    controller: Option<&mut ApiRuntimeController>,
    api: &mut ApiPresentation,
) -> bool {
    let Some(controller) = controller else {
        return false;
    };
    if !controller.drain() {
        return false;
    }
    *api = ApiPresentation::from_controller(controller);
    true
}

fn arm_runtime_curl_copy_feedback(
    feedback: &mut Option<RuntimeCurlCopyFeedback>,
    api: &ApiPresentation,
    command: String,
    now: Instant,
) -> bool {
    let changed = reconcile_runtime_curl_copy_feedback(feedback, api, now);
    if api.curl_command() != Some(command.as_str()) {
        return changed;
    }

    let next = RuntimeCurlCopyFeedback {
        command,
        expires_at: now + RUNTIME_CURL_COPY_FEEDBACK_DURATION,
    };
    let changed = changed || feedback.as_ref() != Some(&next);
    *feedback = Some(next);
    changed
}

fn reconcile_runtime_curl_copy_feedback(
    feedback: &mut Option<RuntimeCurlCopyFeedback>,
    api: &ApiPresentation,
    now: Instant,
) -> bool {
    let should_clear = feedback.as_ref().is_some_and(|feedback| {
        now >= feedback.expires_at || api.curl_command() != Some(feedback.command.as_str())
    });
    if should_clear {
        *feedback = None;
    }
    should_clear
}

fn copy_runtime_curl_with<Copy>(api: &ApiPresentation, copy: Copy) -> Option<String>
where
    Copy: FnOnce(&str) -> bool,
{
    let command = api.curl_command()?;
    copy(command).then(|| command.to_owned())
}

fn copy_runtime_curl_to_pasteboard(api: &ApiPresentation) -> Option<String> {
    copy_runtime_curl_with(api, |command| {
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
            let api = self.ivars().state.borrow().api.clone();
            let Some(command) = copy_runtime_curl_to_pasteboard(&api) else {
                return;
            };

            let mtm = MainThreadMarker::new()
                .expect("AppKit must copy runtime commands on the main thread");
            let mut state = self.ivars().state.borrow_mut();
            if state.arm_runtime_curl_copy_feedback(command, Instant::now()) {
                state.render(self, action_selectors(), mtm);
            }
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
            let (installed, active_model_id) = {
                let state = self.ivars().state.borrow();
                (state.installed.clone(), state.api.active_model_id().map(str::to_owned))
            };
            let model_id = installed
                .borrow()
                .visible_items_for(active_model_id.as_deref())
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

        #[unsafe(method(startApi:))]
        fn start_api(&self, _sender: Option<&NSButton>) {
            #[cfg(not(test))]
            self.perform_api_action(ApiPrimaryActionKind::Start);
        }

        #[unsafe(method(stopApi:))]
        fn stop_api(&self, _sender: Option<&NSButton>) {
            #[cfg(not(test))]
            self.perform_api_action(ApiPrimaryActionKind::Stop);
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
        let (installed, model_paths, allow_copy_chat) = {
            let state = self.ivars().state.borrow();
            (
                state.installed.clone(),
                state.model_paths.clone(),
                !state.shared_service,
            )
        };
        installed_rows::dispatch_native_selected_action(
            &installed,
            action,
            &model_paths,
            allow_copy_chat,
        );
        let mtm = MainThreadMarker::new()
            .expect("AppKit must perform installed actions on the main thread");
        self.ivars()
            .state
            .borrow_mut()
            .render(self, action_selectors(), mtm);
    }

    #[cfg(not(test))]
    fn perform_api_action(&self, requested: ApiPrimaryActionKind) {
        let mtm =
            MainThreadMarker::new().expect("AppKit must perform API actions on the main thread");
        let mut state = self.ivars().state.borrow_mut();
        let selected_model_id = state
            .installed
            .borrow()
            .selected()
            .map(|item| item.id().to_owned());
        let Some(selected_model_id) = selected_model_id else {
            return;
        };
        let action = state.api.primary_action(&selected_model_id);
        if !action.is_enabled() || action.kind() != requested {
            return;
        }
        let observed_runtime = state.snapshot.observed_runtime().cloned();
        let Some(controller) = state.api_runtime.as_mut() else {
            return;
        };
        let accepted = match requested {
            ApiPrimaryActionKind::Start => controller.request_start(selected_model_id),
            ApiPrimaryActionKind::Stop => controller.request_stop(),
        };
        if accepted {
            state.api = ApiPresentation::from_controller_with_observed_runtime(
                controller,
                observed_runtime.as_ref(),
            );
            state.render(self, action_selectors(), mtm);
        }
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

    fn take_exit_resources(&self) -> Option<NativeExitResources> {
        self.ivars().state.borrow_mut().take_exit_resources()
    }

    fn render_current_state(&self, mtm: MainThreadMarker) {
        self.ivars()
            .state
            .borrow_mut()
            .render(self, action_selectors(), mtm);
    }

    fn restore_exit_resources(&self, resources: NativeExitResources, mtm: MainThreadMarker) {
        {
            self.ivars()
                .state
                .borrow_mut()
                .restore_exit_resources(resources);
        }
        self.render_current_state(mtm);
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
        backend_paths: AppPaths,
        _runtime_paths: AppPaths,
        service_client: Option<ServiceClient>,
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
            backend_paths,
            #[cfg(not(test))]
            _runtime_paths,
            service_client,
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

    pub(crate) fn prepare_exit(&mut self, mtm: MainThreadMarker) -> Option<NativeExitResources> {
        let resources = self._target.take_exit_resources();
        if resources.is_some() {
            self._target.render_current_state(mtm);
        }
        resources
    }

    pub(crate) fn restore_exit(&mut self, resources: NativeExitResources, mtm: MainThreadMarker) {
        self._target.restore_exit_resources(resources, mtm);
    }
}

impl Drop for NativePopoverController {
    fn drop(&mut self) {
        #[cfg(not(test))]
        self.timer.shutdown();
        if let Some(mut resources) = self._target.take_exit_resources() {
            let _ = resources.shutdown_runtime();
            let _ = resources.shutdown_backend();
        }
        self.status_item.setMenu(None);
    }
}

struct ProductionActionSelectors {
    runtime_copy: Sel,
    api_start: Sel,
    api_stop: Sel,
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
        api_start: sel!(startApi:),
        api_stop: sel!(stopApi:),
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
        api_start,
        api_stop,
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
        api_start,
        api_stop,
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
    use std::time::{Duration, Instant};

    use loxa::api_runtime::ApiRuntimeActivity;
    use objc2::{sel, ClassType};

    use super::{
        arm_runtime_curl_copy_feedback, copy_runtime_curl_with, production_action_selectors,
        reconcile_runtime_curl_copy_feedback, NativePopoverTarget, ProductionActionSelectors,
    };
    use crate::menu::api_presentation::ApiPresentation;

    fn ready(port: u16) -> ApiPresentation {
        ApiPresentation::ready("demo", port, ApiRuntimeActivity::Loaded)
    }

    #[test]
    fn runtime_copy_dispatches_the_exact_curl_only_for_a_running_endpoint() {
        let running = ready(43123);
        let copied = copy_runtime_curl_with(&running, |_| true);
        assert_eq!(
            copied.as_deref(),
            Some("curl http://127.0.0.1:43123/v1/models")
        );

        assert_eq!(
            copy_runtime_curl_with(&ApiPresentation::idle(), |_| panic!(
                "idle snapshots must not reach the clipboard"
            )),
            None
        );
    }

    #[test]
    fn runtime_copy_feedback_expires_at_exactly_1500_milliseconds() {
        let running = ready(43123);
        let started_at = Instant::now();
        let mut feedback = None;

        assert!(arm_runtime_curl_copy_feedback(
            &mut feedback,
            &running,
            "curl http://127.0.0.1:43123/v1/models".into(),
            started_at,
        ));
        assert_eq!(
            feedback.as_ref().map(|value| value.expires_at),
            Some(started_at + Duration::from_millis(1_500))
        );
        assert!(!reconcile_runtime_curl_copy_feedback(
            &mut feedback,
            &running,
            started_at + Duration::from_millis(1_499),
        ));
        assert!(feedback.is_some());
        assert!(reconcile_runtime_curl_copy_feedback(
            &mut feedback,
            &running,
            started_at + Duration::from_millis(1_500),
        ));
        assert!(feedback.is_none());
    }

    #[test]
    fn repeated_runtime_copy_success_restarts_the_full_feedback_window() {
        let running = ready(43123);
        let started_at = Instant::now();
        let restarted_at = started_at + Duration::from_millis(900);
        let command = "curl http://127.0.0.1:43123/v1/models";
        let mut feedback = None;

        assert!(arm_runtime_curl_copy_feedback(
            &mut feedback,
            &running,
            command.into(),
            started_at,
        ));
        assert!(arm_runtime_curl_copy_feedback(
            &mut feedback,
            &running,
            command.into(),
            restarted_at,
        ));
        assert_eq!(
            feedback.as_ref().map(|value| value.expires_at),
            Some(restarted_at + Duration::from_millis(1_500))
        );
        assert!(!reconcile_runtime_curl_copy_feedback(
            &mut feedback,
            &running,
            started_at + Duration::from_millis(1_500),
        ));
        assert!(feedback.is_some());
    }

    #[test]
    fn runtime_copy_feedback_clears_when_the_runtime_disappears() {
        let running = ready(43123);
        let started_at = Instant::now();
        let mut feedback = None;

        assert!(arm_runtime_curl_copy_feedback(
            &mut feedback,
            &running,
            "curl http://127.0.0.1:43123/v1/models".into(),
            started_at,
        ));

        assert!(reconcile_runtime_curl_copy_feedback(
            &mut feedback,
            &ApiPresentation::idle(),
            started_at,
        ));
        assert!(feedback.is_none());
    }

    #[test]
    fn runtime_copy_feedback_rejects_a_replaced_endpoint() {
        let first = ready(43123);
        let replacement = ready(43124);
        let started_at = Instant::now();
        let command = "curl http://127.0.0.1:43123/v1/models";
        let mut feedback = None;

        assert!(arm_runtime_curl_copy_feedback(
            &mut feedback,
            &first,
            command.into(),
            started_at,
        ));
        assert!(reconcile_runtime_curl_copy_feedback(
            &mut feedback,
            &replacement,
            started_at + Duration::from_millis(1),
        ));
        assert!(feedback.is_none());
        assert!(!arm_runtime_curl_copy_feedback(
            &mut feedback,
            &replacement,
            command.into(),
            started_at + Duration::from_millis(2),
        ));
        assert!(feedback.is_none());
    }

    #[test]
    fn failed_runtime_pasteboard_write_never_arms_feedback() {
        let running = ready(43123);
        let mut feedback = None;

        let copied = copy_runtime_curl_with(&running, |_| false);
        if let Some(command) = copied {
            let _ =
                arm_runtime_curl_copy_feedback(&mut feedback, &running, command, Instant::now());
        }

        assert!(feedback.is_none());
    }

    #[test]
    fn production_target_exposes_catalog_and_transfer_actions() {
        let class = NativePopoverTarget::class();

        for action in [
            sel!(copyRuntimeCurl:),
            sel!(startApi:),
            sel!(stopApi:),
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
            api_start,
            api_stop,
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
        assert_eq!(api_start, sel!(startApi:));
        assert_eq!(api_stop, sel!(stopApi:));
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
