#![allow(dead_code)]

#[cfg(target_os = "macos")]
mod menu {
    pub(crate) mod presentation {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/menu/presentation.rs"
        ));
    }

    pub(crate) mod macos {
        pub(crate) mod rows {
            include!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/src/menu/macos/rows.rs"
            ));
            use crate::menu::presentation::Fixture;
            use objc2::sel;
            use objc2_app_kit::NSEventModifierFlags;

            pub(crate) fn assert_native_layout_contract(mtm: MainThreadMarker) {
                for (name, fixture, expected_height, expected_action_button_count) in [
                    ("recommendation", Fixture::Empty, 300.0, 1),
                    ("installed", Fixture::Installed, 223.0, 0),
                    ("recovery", Fixture::Invalid, 223.0, 0),
                    ("transfer", Fixture::Downloading, 283.0, 4),
                ] {
                    let PopoverContent {
                        view,
                        quit_button,
                        action_buttons,
                        ..
                    } = MenuRows::build(&fixture.snapshot(), None, layout_fixture_actions(), mtm);
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
            }

            fn layout_fixture_actions() -> Actions {
                Actions {
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
        }
    }
}

#[cfg(target_os = "macos")]
fn main() {
    let mtm = objc2::MainThreadMarker::new()
        .expect("native popover layout coverage must run on the main thread");
    menu::macos::rows::assert_native_layout_contract(mtm);
}

#[cfg(not(target_os = "macos"))]
fn main() {}
