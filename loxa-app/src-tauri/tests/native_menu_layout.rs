#![allow(dead_code)]

#[cfg(target_os = "macos")]
mod menu {
    pub(crate) mod catalog {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/menu/catalog.rs"));
    }

    pub(crate) mod presentation {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/menu/presentation.rs"
        ));
    }

    pub(crate) mod macos {
        pub(crate) mod catalog_rows {
            include!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/src/menu/macos/catalog_rows.rs"
            ));
        }

        pub(crate) mod timer {
            include!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/src/menu/macos/timer.rs"
            ));
        }

        pub(crate) mod rows {
            include!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/src/menu/macos/rows.rs"
            ));
            use crate::menu::presentation::Fixture;
            use objc2::runtime::NSObjectProtocol;
            use objc2::sel;
            use objc2::ClassType;
            use objc2_app_kit::{NSEventModifierFlags, NSSearchField};

            pub(crate) fn assert_native_layout_contract(mtm: MainThreadMarker) {
                for (name, fixture, expected_height, expected_action_button_count) in [
                    ("recommendation", Fixture::Empty, 393.0, 1),
                    ("installed", Fixture::Installed, 316.0, 0),
                    ("recovery", Fixture::Invalid, 316.0, 0),
                    ("transfer", Fixture::Downloading, 376.0, 4),
                ] {
                    let PopoverContent {
                        view,
                        quit_button,
                        action_buttons,
                        ..
                    } = MenuRows::build(
                        &fixture.snapshot(),
                        &crate::menu::catalog::CatalogState::default(),
                        None,
                        layout_fixture_actions(),
                        mtm,
                    );
                    view.layoutSubtreeIfNeeded();

                    assert_eq!(view.frame().size.width, 300.0, "{name} width");
                    assert_eq!(view.frame().size.height, expected_height, "{name} height");
                    // SAFETY: MenuRows::build adds the retained Quit button to
                    // its retained row before returning this content view.
                    let quit_row = unsafe { quit_button.superview() }
                        .expect("the Quit button must stay attached to its row");
                    assert_eq!(
                        quit_row.frame().origin.y + quit_button.frame().origin.y,
                        8.0,
                        "{name} Quit gap"
                    );

                    assert_eq!(
                        action_buttons.len(),
                        expected_action_button_count,
                        "{name} actionable button count"
                    );
                    for button in action_buttons.iter().chain(std::iter::once(&quit_button)) {
                        assert!(
                            button.refusesFirstResponder(),
                            "{name} actionable buttons must refuse first responder"
                        );
                        assert!(
                            !button.isHighlighted(),
                            "{name} actionable buttons must start neutral"
                        );
                    }
                    assert_eq!(
                        quit_button.keyEquivalent().to_string(),
                        "q",
                        "{name} Quit key"
                    );
                    assert_eq!(
                        quit_button.keyEquivalentModifierMask(),
                        NSEventModifierFlags::Command,
                        "{name} Quit modifier"
                    );
                }

                let search = MenuRows::build(
                    &Fixture::Empty.snapshot(),
                    &crate::menu::catalog::CatalogState::default(),
                    None,
                    layout_fixture_actions(),
                    mtm,
                );
                assert_eq!(
                    count_search_fields(&search.view),
                    1,
                    "the native product menu must contain exactly one NSSearchField"
                );
            }

            fn layout_fixture_actions() -> Actions {
                Actions {
                    search: sel!(fixtureNoop:),
                    repository: sel!(fixtureNoop:),
                    candidate: sel!(fixtureNoop:),
                    transfer: sel!(fixtureNoop:),
                    pause_transfer: sel!(fixtureNoop:),
                    start: sel!(fixtureNoop:),
                    pause: sel!(fixtureNoop:),
                    resume: sel!(fixtureNoop:),
                    retry: sel!(fixtureNoop:),
                    cancel: sel!(fixtureNoop:),
                    keep_partial: sel!(fixtureNoop:),
                    discard_partial: sel!(fixtureNoop:),
                    quit: sel!(fixtureNoop:),
                }
            }

            fn count_search_fields(view: &NSView) -> usize {
                usize::from(view.isKindOfClass(NSSearchField::class()))
                    + view
                        .subviews()
                        .iter()
                        .map(|child| count_search_fields(&child))
                        .sum::<usize>()
            }
        }

        pub(crate) fn assert_native_timer_contract(mtm: objc2::MainThreadMarker) {
            use std::cell::Cell;
            use std::rc::Rc;

            use objc2::rc::Weak;
            use objc2_foundation::NSObject;

            use timer::{weak_callback, ObservationTimer};

            let ticks = Rc::new(Cell::new(0));
            let observed = NSObject::new();
            let callback_ticks = ticks.clone();
            let mut timer = ObservationTimer::schedule(
                3_600.0,
                weak_callback(&observed, move |_| {
                    callback_ticks.set(callback_ticks.get() + 1)
                }),
                mtm,
            );
            let timer_handle = timer.timer.as_ref().unwrap().clone();
            timer_handle.fire();
            assert_eq!(ticks.get(), 1);
            drop(observed);
            timer_handle.fire();
            assert_eq!(ticks.get(), 1);
            timer.shutdown();

            let ticks = Rc::new(Cell::new(0));
            let observed = NSObject::new();
            let observed_weak = Weak::from_retained(&observed);
            let callback_ticks = ticks.clone();
            let mut timer = ObservationTimer::schedule(
                3_600.0,
                weak_callback(&observed, move |_| {
                    callback_ticks.set(callback_ticks.get() + 1)
                }),
                mtm,
            );
            let timer_handle = timer.timer.as_ref().unwrap().clone();
            let target_weak = Weak::from_retained(timer.callback_target.as_ref().unwrap());
            timer_handle.fire();
            assert_eq!(ticks.get(), 1);
            timer.shutdown();
            assert!(!timer_handle.isValid());
            assert!(target_weak.load().is_none());
            assert!(observed_weak.load().is_some());
            timer_handle.fire();
            assert_eq!(ticks.get(), 1);
        }
    }
}

#[cfg(target_os = "macos")]
fn main() {
    let mtm = objc2::MainThreadMarker::new()
        .expect("native popover layout coverage must run on the main thread");
    menu::macos::rows::assert_native_layout_contract(mtm);
    menu::macos::assert_native_timer_contract(mtm);
}

#[cfg(not(target_os = "macos"))]
fn main() {}
