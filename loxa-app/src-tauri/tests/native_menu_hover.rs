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

        pub(crate) mod rows {
            include!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/src/menu/macos/rows.rs"
            ));
            use objc2::sel;
            use objc2::ClassType;
            use objc2_app_kit::{NSEventModifierFlags, NSEventType};

            pub(crate) fn assert_native_hover_contract(mtm: MainThreadMarker) {
                let row = hover_row_shell(44.0, mtm);
                row.updateTrackingAreas();
                assert_eq!(
                    row.trackingAreas().count(),
                    1,
                    "the hover row must install one AppKit tracking area"
                );
                assert!(
                    !row.draws_hover_background(),
                    "an actionable row starts without a hover background"
                );

                let entered = pointer_event();
                row.mouse_entered(sel!(mouseEntered:), &entered);
                assert!(
                    row.draws_hover_background(),
                    "the AppKit mouse-entered callback must show the hover background"
                );

                let exited = pointer_event();
                row.mouse_exited(sel!(mouseExited:), &exited);
                assert!(
                    !row.draws_hover_background(),
                    "the AppKit mouse-exited callback must remove the hover background"
                );
                assert_eq!(row.hover_radius(), 6.0);
                assert_eq!(row.class(), HoverRowView::class());

                let hover_bounds = row.hover_bounds();
                assert_eq!(hover_bounds.origin.x, 5.0);
                assert_eq!(hover_bounds.origin.y, 0.0);
                assert_eq!(hover_bounds.size.width, 290.0);
                assert_eq!(hover_bounds.size.height, 44.0);

                let passive = row_shell(44.0, mtm);
                assert_eq!(passive.class(), NSView::class());
            }

            fn pointer_event() -> Retained<NSEvent> {
                NSEvent::mouseEventWithType_location_modifierFlags_timestamp_windowNumber_context_eventNumber_clickCount_pressure(
                    NSEventType::LeftMouseDown,
                    NSPoint::new(0.0, 0.0),
                    NSEventModifierFlags::empty(),
                    0.0,
                    0,
                    None,
                    0,
                    0,
                    0.0,
                )
                .expect("the hover harness must construct a native mouse event")
            }
        }
    }
}

#[cfg(target_os = "macos")]
fn main() {
    let mtm = objc2::MainThreadMarker::new()
        .expect("native popover hover characterization must run on the main thread");
    menu::macos::rows::assert_native_hover_contract(mtm);
}

#[cfg(not(target_os = "macos"))]
fn main() {}
