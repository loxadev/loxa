#![allow(dead_code, unused_imports)]

#[cfg(target_os = "macos")]
mod menu {
    pub(crate) mod macos {
        pub(crate) mod timer {
            include!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/src/menu/macos/timer.rs"
            ));
        }

        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use std::time::{Duration, Instant};

        use objc2::MainThreadMarker;
        use objc2_app_kit::NSEventTrackingRunLoopMode;
        use objc2_foundation::{NSDate, NSRunLoop};

        pub(crate) fn assert_observation_timer_runs_during_tracking(mtm: MainThreadMarker) {
            let callbacks = Arc::new(AtomicUsize::new(0));
            let observed = Arc::clone(&callbacks);
            let mut timer = timer::ObservationTimer::schedule(
                0.01,
                move || {
                    observed.fetch_add(1, Ordering::SeqCst);
                },
                mtm,
            );
            let run_loop = NSRunLoop::mainRunLoop();

            run_tracking_mode(&run_loop, Duration::from_millis(300));
            let callback_count = callbacks.load(Ordering::SeqCst);
            assert!(
                callback_count > 0,
                "observation callbacks must continue while AppKit tracks a native menu"
            );

            timer.shutdown();
            run_tracking_mode(&run_loop, Duration::from_millis(100));
            assert_eq!(
                callbacks.load(Ordering::SeqCst),
                callback_count,
                "shutdown must prevent later observation callbacks"
            );
        }

        fn run_tracking_mode(run_loop: &NSRunLoop, duration: Duration) {
            let deadline = Instant::now() + duration;
            while Instant::now() < deadline {
                let until = NSDate::dateWithTimeIntervalSinceNow(0.01);
                // SAFETY: AppKit exposes this immutable process-wide mode constant.
                let mode = unsafe { NSEventTrackingRunLoopMode };
                run_loop.runMode_beforeDate(mode, &until);
            }
        }
    }
}

#[cfg(target_os = "macos")]
fn main() {
    use objc2::MainThreadMarker;
    use objc2_app_kit::NSApplication;

    let mtm = MainThreadMarker::new()
        .expect("native observation timer coverage must run on the main thread");
    let _application = NSApplication::sharedApplication(mtm);
    menu::macos::assert_observation_timer_runs_during_tracking(mtm);
}

#[cfg(not(target_os = "macos"))]
fn main() {}
