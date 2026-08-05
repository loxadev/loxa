use objc2::rc::{Retained, Weak};
use objc2::{define_class, msg_send, sel, DefinedClass, MainThreadMarker, MainThreadOnly, Message};
use objc2_foundation::{NSObject, NSObjectProtocol, NSTimer};

pub(super) fn weak_callback<T: Message + 'static>(
    target: &Retained<T>,
    callback: impl Fn(&T) + 'static,
) -> impl Fn() + 'static {
    let target = Weak::from_retained(target);
    move || {
        if let Some(target) = target.load() {
            callback(&target);
        }
    }
}

pub(super) struct NativeTimerTargetIvars {
    callback: Box<dyn Fn()>,
}

define_class!(
    // SAFETY: NSObject has no subclassing requirements, and this class has no Drop implementation.
    #[unsafe(super = NSObject)]
    #[thread_kind = MainThreadOnly]
    #[ivars = NativeTimerTargetIvars]
    pub(super) struct NativeTimerTarget;

    // SAFETY: NSObjectProtocol has no safety requirements.
    unsafe impl NSObjectProtocol for NativeTimerTarget {}

    impl NativeTimerTarget {
        #[unsafe(method(drainObservation:))]
        fn drain_observation(&self, _timer: Option<&NSTimer>) {
            (self.ivars().callback)();
        }
    }
);

impl NativeTimerTarget {
    fn new(callback: impl Fn() + 'static, mtm: MainThreadMarker) -> Retained<Self> {
        let this = Self::alloc(mtm).set_ivars(NativeTimerTargetIvars {
            callback: Box::new(callback),
        });

        // SAFETY: NSObject's init selector has the expected signature.
        unsafe { msg_send![super(this), init] }
    }
}

pub(super) struct ObservationTimer {
    pub(super) timer: Option<Retained<NSTimer>>,
    pub(super) callback_target: Option<Retained<NativeTimerTarget>>,
}

impl ObservationTimer {
    pub(super) fn schedule(
        interval: f64,
        callback: impl Fn() + 'static,
        mtm: MainThreadMarker,
    ) -> Self {
        let callback_target = NativeTimerTarget::new(callback, mtm);
        // SAFETY: the retained target implements drainObservation:, and shutdown
        // invalidates the scheduled timer before releasing that target.
        let timer = unsafe {
            NSTimer::scheduledTimerWithTimeInterval_target_selector_userInfo_repeats(
                interval,
                &callback_target,
                sel!(drainObservation:),
                None,
                true,
            )
        };
        Self {
            timer: Some(timer),
            callback_target: Some(callback_target),
        }
    }

    pub(super) fn shutdown(&mut self) {
        if let Some(timer) = self.timer.take() {
            timer.invalidate();
        }
        self.callback_target.take();
    }
}
