use objc2::rc::Retained;
use objc2::runtime::{AnyObject, ProtocolObject};
use objc2::{define_class, msg_send, sel, DefinedClass, MainThreadMarker, MainThreadOnly};
use objc2_app_kit::{
    NSAccessibility, NSButton, NSMenu, NSMenuDelegate, NSMenuItem, NSStatusItem, NSView,
};
use objc2_foundation::{ns_string, NSObject, NSObjectProtocol, NSPoint, NSRect, NSSize};
use tauri::AppHandle;

const CONTROL_TITLE: &str = "Verify native control";
const CONTROL_ACTIVATED_TITLE: &str = "Native control activated";

struct NativeMenuDelegateIvars {
    button: Retained<NSButton>,
}

define_class!(
    // SAFETY: NSObject has no subclassing requirements, and this class has no Drop implementation.
    #[unsafe(super = NSObject)]
    #[thread_kind = MainThreadOnly]
    #[ivars = NativeMenuDelegateIvars]
    struct NativeMenuDelegate;

    // SAFETY: NSObjectProtocol has no safety requirements.
    unsafe impl NSObjectProtocol for NativeMenuDelegate {}

    // SAFETY: NSMenuDelegate has no safety requirements.
    unsafe impl NSMenuDelegate for NativeMenuDelegate {
        #[allow(non_snake_case)]
        #[unsafe(method(menuWillOpen:))]
        fn menuWillOpen(&self, _menu: &NSMenu) {
            self.ivars().button.setTitle(ns_string!(CONTROL_TITLE));
        }
    }
);

impl NativeMenuDelegate {
    fn new(button: Retained<NSButton>, mtm: MainThreadMarker) -> Retained<Self> {
        let this = Self::alloc(mtm).set_ivars(NativeMenuDelegateIvars { button });

        // SAFETY: NSObject's init selector has the expected signature.
        unsafe { msg_send![super(this), init] }
    }
}

struct NativeMenuTargetIvars {
    app_handle: AppHandle,
    button: Retained<NSButton>,
}

define_class!(
    // SAFETY: NSObject has no subclassing requirements, and this class has no Drop implementation.
    #[unsafe(super = NSObject)]
    #[thread_kind = MainThreadOnly]
    #[ivars = NativeMenuTargetIvars]
    struct NativeMenuTarget;

    // SAFETY: NSObjectProtocol has no safety requirements.
    unsafe impl NSObjectProtocol for NativeMenuTarget {}

    impl NativeMenuTarget {
        #[unsafe(method(activateNativeControl:))]
        fn activate_native_control(&self, _sender: Option<&AnyObject>) {
            self.ivars()
                .button
                .setTitle(ns_string!(CONTROL_ACTIVATED_TITLE));
        }

        #[unsafe(method(quit:))]
        fn quit(&self, _sender: Option<&AnyObject>) {
            self.ivars().app_handle.exit(0);
        }
    }
);

impl NativeMenuTarget {
    fn new(
        app_handle: AppHandle,
        button: Retained<NSButton>,
        mtm: MainThreadMarker,
    ) -> Retained<Self> {
        let this = Self::alloc(mtm).set_ivars(NativeMenuTargetIvars { app_handle, button });

        // SAFETY: NSObject's init selector has the expected signature.
        unsafe { msg_send![super(this), init] }
    }
}

pub(crate) struct NativeMenuController {
    status_item: Retained<NSStatusItem>,
    _menu: Retained<NSMenu>,
    _delegate: Retained<NativeMenuDelegate>,
    _target: Retained<NativeMenuTarget>,
}

impl NativeMenuController {
    pub(crate) fn attach(
        status_item: Retained<NSStatusItem>,
        app_handle: AppHandle,
        mtm: MainThreadMarker,
    ) -> Self {
        let button = NSButton::new(mtm);
        button.setTitle(ns_string!(CONTROL_TITLE));
        button.setFrame(NSRect::new(
            NSPoint::new(12.0, 8.0),
            NSSize::new(232.0, 28.0),
        ));
        button.setRefusesFirstResponder(false);
        button.setToolTip(Some(ns_string!("Activate the Loxa native menu control")));
        button.setAccessibilityLabel(Some(ns_string!("Verify native menu control")));

        let target = NativeMenuTarget::new(app_handle, button.clone(), mtm);
        // SAFETY: NativeMenuTarget implements both selectors with the expected Objective-C ABI.
        unsafe {
            button.setTarget(Some(&target));
            button.setAction(Some(sel!(activateNativeControl:)));
        }

        let row = NSView::initWithFrame(
            NSView::alloc(mtm),
            NSRect::new(NSPoint::new(0.0, 0.0), NSSize::new(256.0, 44.0)),
        );
        row.addSubview(&button);
        let row_item = NSMenuItem::new(mtm);
        row_item.setView(Some(&row));

        let menu = NSMenu::initWithTitle(NSMenu::alloc(mtm), ns_string!("Loxa"));
        menu.setAutoenablesItems(false);
        let delegate = NativeMenuDelegate::new(button, mtm);
        menu.setDelegate(Some(ProtocolObject::from_ref(&*delegate)));
        menu.addItem(&row_item);
        menu.addItem(&NSMenuItem::separatorItem(mtm));

        let quit_item = unsafe {
            NSMenuItem::initWithTitle_action_keyEquivalent(
                NSMenuItem::alloc(mtm),
                ns_string!("Quit Loxa"),
                Some(sel!(quit:)),
                ns_string!("q"),
            )
        };
        // SAFETY: NativeMenuTarget implements quit: with the expected Objective-C ABI.
        unsafe { quit_item.setTarget(Some(&target)) };
        menu.addItem(&quit_item);
        status_item.setMenu(Some(&menu));

        Self {
            status_item,
            _menu: menu,
            _delegate: delegate,
            _target: target,
        }
    }
}

impl Drop for NativeMenuController {
    fn drop(&mut self) {
        self.status_item.setMenu(None);
    }
}
