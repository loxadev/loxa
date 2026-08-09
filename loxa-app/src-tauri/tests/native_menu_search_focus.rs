#![allow(dead_code, unused_imports)]

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

    pub(crate) mod progress {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/menu/progress.rs"));
    }

    pub(crate) mod macos {
        pub(crate) mod catalog_rows {
            include!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/src/menu/macos/catalog_rows.rs"
            ));
        }

        pub(crate) fn assert_search_focus_survives_result_rebuild(mtm: objc2::MainThreadMarker) {
            use objc2::MainThreadOnly;
            use objc2_app_kit::NSWindow;
            use objc2_foundation::{NSRange, NSString};

            use crate::menu::catalog::{CatalogEvent, CatalogState, RepositoryItem};

            let mut state = CatalogState::default();
            let generation = state
                .submit_search("bartowski")
                .expect("the focus fixture starts a real search")
                .generation();
            let first = catalog_rows::build(&state, None, fixture_actions(), mtm);
            let window = unsafe { NSWindow::init(NSWindow::alloc(mtm)) };
            window.setContentView(Some(&first.view));
            assert!(
                window.makeFirstResponder(Some(&first.search)),
                "the test window must begin editing the product search field"
            );
            let draft = "bartowski/qwen";
            first.search.setStringValue(&NSString::from_str(draft));
            let selected = NSRange::new(2, 4);
            first
                .search
                .currentEditor()
                .expect("the product search field must own a field editor")
                .setSelectedRange(selected);
            let focus = first
                .capture_search_focus()
                .expect("the active product search field must expose its edit state");

            assert!(state.apply(CatalogEvent::Repositories {
                generation,
                repositories: vec![RepositoryItem::new(
                    "bartowski/example-model".into(),
                    Some("42 downloads".into()),
                )],
            }));
            let replacement = catalog_rows::build(&state, None, fixture_actions(), mtm);
            window.setContentView(Some(&replacement.view));
            replacement.restore_search_focus(focus);

            assert_eq!(
                replacement
                    .search
                    .currentEditor()
                    .expect("the replacement search field must remain first responder")
                    .selectedRange(),
                selected,
                "the replacement field editor must retain the nontrivial selection"
            );
            assert_eq!(
                replacement.search.stringValue().to_string(),
                draft,
                "an unsent draft must survive an unavoidable result rebuild"
            );
        }

        fn fixture_actions() -> catalog_rows::CatalogActions {
            use objc2::sel;

            catalog_rows::CatalogActions {
                search: sel!(fixtureNoop:),
                repository: sel!(fixtureNoop:),
                candidate: sel!(fixtureNoop:),
                transfer: sel!(fixtureNoop:),
                pause: sel!(fixtureNoop:),
            }
        }
    }
}

#[cfg(target_os = "macos")]
fn main() {
    let mtm = objc2::MainThreadMarker::new()
        .expect("native search-focus coverage must run on the main thread");
    menu::macos::assert_search_focus_survives_result_rebuild(mtm);
}

#[cfg(not(target_os = "macos"))]
fn main() {}
