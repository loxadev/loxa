#![allow(dead_code, unused_imports)]

#[cfg(target_os = "macos")]
mod app {
    pub(crate) fn request_native_shell_exit(_app_handle: &tauri::AppHandle) {}
}

#[cfg(target_os = "macos")]
mod menu {
    pub(crate) mod catalog {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/menu/catalog.rs"));
    }

    pub(crate) mod installed {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/menu/installed.rs"
        ));
    }

    pub(crate) mod incomplete {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/menu/incomplete.rs"
        ));
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

    pub(crate) mod observation {
        use std::time::Instant;

        use crate::menu::catalog::CatalogCommand;

        pub(crate) struct BackendClient;

        impl BackendClient {
            pub(crate) fn request_popover_open(&mut self, _now: Instant) -> bool {
                false
            }

            pub(crate) fn dispatch(&mut self, _command: CatalogCommand) -> bool {
                false
            }

            pub(crate) fn shutdown(&mut self) {}

            pub(crate) fn request_pause(&self, _generation: u64) -> bool {
                false
            }

            pub(crate) fn prepare_discard(&mut self, _model_id: String) -> bool {
                false
            }

            pub(crate) fn keep_discard(&mut self, _model_id: String) -> bool {
                false
            }

            pub(crate) fn confirm_discard(&mut self, _model_id: String) -> bool {
                false
            }
        }
    }

    pub(crate) mod macos {
        pub(crate) mod catalog_rows {
            include!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/src/menu/macos/catalog_rows.rs"
            ));
        }

        pub(crate) mod installed_rows {
            include!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/src/menu/macos/installed_rows.rs"
            ));
        }

        pub(crate) mod incomplete_rows {
            include!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/src/menu/macos/incomplete_rows.rs"
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
            use crate::menu::catalog::{CandidateItem, CatalogEvent, RepositoryItem};
            use crate::menu::incomplete::IncompleteItem;
            use crate::menu::installed::{
                InstalledFeedback, InstalledInventoryError, InstalledItem, InstalledState,
            };
            use crate::menu::presentation::Fixture;
            use objc2::runtime::NSObjectProtocol;
            use objc2::sel;
            use objc2::ClassType;
            use objc2_app_kit::{NSEventModifierFlags, NSLineBreakMode, NSSearchField};
            use std::time::{Duration, Instant};

            pub(crate) fn assert_native_layout_contract(mtm: MainThreadMarker) {
                for (name, fixture, expected_height, expected_action_button_count) in [
                    ("recommendation", Fixture::Empty, 349.0, 1),
                    ("installed", Fixture::Installed, 272.0, 0),
                    ("recovery", Fixture::Invalid, 272.0, 0),
                    ("transfer", Fixture::Downloading, 332.0, 4),
                ] {
                    let PopoverContent {
                        view,
                        quit_button,
                        action_buttons,
                        ..
                    } = MenuRows::build(
                        &fixture.snapshot(),
                        &crate::menu::catalog::CatalogState::default(),
                        &crate::menu::incomplete::IncompleteState::default(),
                        &InstalledState::default(),
                        None,
                        layout_fixture_actions(),
                        mtm,
                    );
                    view.layoutSubtreeIfNeeded();

                    assert_eq!(view.frame().size.width, 360.0, "{name} width");
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
                    for button in &action_buttons {
                        assert!(
                            !button.refusesFirstResponder(),
                            "{name} actionable buttons must accept keyboard focus"
                        );
                        assert!(
                            !button.isHighlighted(),
                            "{name} actionable buttons must start neutral"
                        );
                    }
                    assert!(
                        quit_button.refusesFirstResponder(),
                        "{name} Quit keeps its passive key-equivalent behavior"
                    );
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

                let running = Fixture::Running
                    .snapshot()
                    .with_running_port(43123)
                    .unwrap();
                let runtime = MenuRows::build(
                    &running,
                    &crate::menu::catalog::CatalogState::default(),
                    &crate::menu::incomplete::IncompleteState::default(),
                    &InstalledState::default(),
                    None,
                    layout_fixture_actions(),
                    mtm,
                );
                let runtime_text = visible_text_values(&runtime.view);
                assert!(
                    runtime_text.contains(&"API · 127.0.0.1:43123".into()),
                    "running header omitted its loopback endpoint: {runtime_text:?}"
                );
                assert_eq!(runtime.action_buttons.len(), 1);
                assert_eq!(
                    runtime.action_buttons[0]
                        .accessibilityLabel()
                        .map(|label| label.to_string()),
                    Some("Copy API curl command".into())
                );
                assert!(runtime.action_buttons[0].image().is_some());
                assert!(!runtime.action_buttons[0].refusesFirstResponder());

                let search = MenuRows::build(
                    &Fixture::Empty.snapshot(),
                    &crate::menu::catalog::CatalogState::default(),
                    &crate::menu::incomplete::IncompleteState::default(),
                    &InstalledState::default(),
                    None,
                    layout_fixture_actions(),
                    mtm,
                );
                assert_eq!(
                    count_search_fields(&search.view),
                    1,
                    "the native product menu must contain exactly one NSSearchField"
                );
                let search_field = find_search_field(&search.view)
                    .expect("the native product menu must retain its search field");
                assert!(
                    !search_field.sendsWholeSearchString(),
                    "typing must use AppKit's delayed edit action without Return"
                );
                assert!(
                    !search_field.sendsSearchStringImmediately(),
                    "live search must use AppKit's native delay"
                );

                assert_catalog_browsing_contract(mtm);
                assert_installed_rows_and_priority(mtm);
                assert_incomplete_rows_are_bounded_accessible_and_hidden_while_browsing(mtm);
                assert_empty_inventory_error_is_rendered(mtm);
                assert_known_installed_row_outlives_cold_inventory_error(mtm);
                super::controller::assert_feedback_close_rebuild_contract(mtm);
            }

            fn assert_catalog_browsing_contract(mtm: MainThreadMarker) {
                let mut catalog = CatalogState::default();
                let generation = catalog.submit_search("bartowski").unwrap().generation();
                let searching = catalog_rows::build(&catalog, None, catalog_fixture_actions(), mtm);
                assert_eq!(searching.view.frame().size.width, 360.0);
                assert!(text_values(&searching.view).contains(&"Models".into()));

                let repository = "bartowski/a-very-long-model-identifier-for-middle-truncation";
                assert!(catalog.apply(CatalogEvent::Repositories {
                    generation,
                    repositories: vec![RepositoryItem::new(
                        repository.into(),
                        Some("42 downloads".into()),
                    )],
                }));
                let repositories =
                    catalog_rows::build(&catalog, None, catalog_fixture_actions(), mtm);
                assert_eq!(repositories.action_buttons.len(), 1);
                assert!(!repositories.action_buttons[0].refusesFirstResponder());
                assert_eq!(
                    repositories.primary_labels[0].stringValue().to_string(),
                    repository
                );
                assert_eq!(
                    repositories.secondary_labels[0].stringValue().to_string(),
                    "42 downloads"
                );
                assert_eq!(
                    repositories.primary_labels[0]
                        .cell()
                        .expect("repository title has a native text cell")
                        .lineBreakMode(),
                    NSLineBreakMode::ByTruncatingMiddle
                );
                assert_eq!(
                    repositories.primary_labels[0].alignment(),
                    NSTextAlignment::Left
                );
                assert_eq!(
                    repositories.primary_labels[0]
                        .toolTip()
                        .map(|value| value.to_string()),
                    Some(repository.into())
                );
                assert_eq!(
                    repositories.primary_labels[0].textColor(),
                    Some(NSColor::labelColor())
                );
                assert!(count_image_views(&repositories.view) >= 1);

                assert!(catalog.inspect_repository(0).is_some());
                let inspecting =
                    catalog_rows::build(&catalog, None, catalog_fixture_actions(), mtm);
                let inspecting_text = text_values(&inspecting.view);
                assert!(inspecting_text.contains(&"Choose a GGUF file".into()));
                assert!(inspecting_text.contains(&"Loading files…".into()));

                let candidate = "a-very-long-model-file-name-q4-k-m.gguf";
                assert!(catalog.apply(CatalogEvent::Candidates {
                    generation,
                    repo: repository.into(),
                    revision: "0123456789abcdef0123456789abcdef01234567".into(),
                    candidates: vec![CandidateItem::new(candidate.into(), Some(500_000))
                        .with_installed_model_id("custom-model".into())],
                }));
                assert!(catalog.select_candidate(0));
                let selected = catalog_rows::build(&catalog, None, catalog_fixture_actions(), mtm);
                assert_eq!(
                    selected.primary_labels[0].stringValue().to_string(),
                    candidate
                );
                assert_eq!(
                    selected.action_buttons[0]
                        .accessibilityLabel()
                        .map(|label| label.to_string())
                        .as_deref(),
                    Some("a-very-long-model-file-name-q4-k-m.gguf; 500.0 KB; Installed; selected")
                );
                assert!(all_image_views(&selected.view)
                    .iter()
                    .all(|image| !image.isAccessibilityElement()));
                assert_eq!(
                    selected.secondary_labels[0].stringValue().to_string(),
                    "500.0 KB · Installed"
                );
                assert_eq!(
                    selected.action_buttons.last().unwrap().title().to_string(),
                    "Check installed"
                );
                assert!(!text_values(&selected.view)
                    .iter()
                    .any(|text| text.starts_with("Selected ")));
                assert!(count_image_views(&selected.view) >= 2);

                assert!(catalog.start_transfer().is_some());
                let resolving = catalog_rows::build(&catalog, None, catalog_fixture_actions(), mtm);
                let resolving_text = text_values(&resolving.view);
                assert!(resolving_text.contains(&"Downloading model".into()));
                assert!(resolving_text.contains(&format!("Preparing {candidate}…")));
                assert!(resolving.primary_labels.is_empty());
                assert!(!resolving_text.contains(&candidate.into()));
                assert_eq!(
                    resolving
                        .action_buttons
                        .iter()
                        .map(|button| button.title().to_string())
                        .collect::<Vec<_>>(),
                    ["Pause"]
                );

                let start = Instant::now();
                assert!(catalog.apply_at(
                    CatalogEvent::Progress {
                        generation,
                        stage: crate::menu::catalog::TransferStage::Transferring,
                        transferred_bytes: 15_900_000,
                        total_bytes: 88_200_000,
                    },
                    start,
                ));
                let first_progress =
                    catalog_rows::build(&catalog, None, catalog_fixture_actions(), mtm);
                let first_text = text_values(&first_progress.view);
                assert!(first_text.contains(&"Downloading model".into()));
                assert!(first_text.contains(&"18% · 15.9 MB of 88.2 MB".into()));
                assert!(!first_text.iter().any(|text| text.contains("/s")));
                assert!(first_progress.primary_labels.is_empty());

                assert!(catalog.apply_at(
                    CatalogEvent::Progress {
                        generation,
                        stage: crate::menu::catalog::TransferStage::Transferring,
                        transferred_bytes: 20_900_000,
                        total_bytes: 88_200_000,
                    },
                    start + Duration::from_secs(1),
                ));
                let measured = catalog_rows::build(&catalog, None, catalog_fixture_actions(), mtm);
                let measured_text = text_values(&measured.view);
                assert!(!measured_text.contains(&"18% · 15.9 MB of 88.2 MB".into()));
                assert!(measured_text.contains(&"24% · 20.9 MB of 88.2 MB".into()));
                assert!(measured_text.contains(&"5.0 MB/s · 14s remaining".into()));
                assert!(measured.primary_labels.is_empty());
                assert!(!measured_text.contains(&candidate.into()));
                let headline = find_text_field(&measured.view, "24% · 20.9 MB of 88.2 MB")
                    .expect("the transfer card must render its human progress headline");
                let detail = find_text_field(&measured.view, "5.0 MB/s · 14s remaining")
                    .expect("the transfer card must render its measured speed and ETA");
                let pause = measured
                    .action_buttons
                    .iter()
                    .find(|button| button.title().to_string() == "Pause")
                    .expect("resolving and progress retain the exact Pause action");
                assert_eq!(headline.frame().size.width, 328.0);
                assert_eq!(detail.frame().size.width, 328.0);
                assert!(
                    detail.frame().origin.y >= pause.frame().origin.y + pause.frame().size.height,
                    "full-width progress labels must sit above the separate Pause control"
                );

                for (stage, expected) in [
                    (
                        crate::menu::catalog::TransferStage::Verifying,
                        "Verifying download…",
                    ),
                    (
                        crate::menu::catalog::TransferStage::Publishing,
                        "Finishing installation…",
                    ),
                ] {
                    assert!(catalog.apply_at(
                        CatalogEvent::Progress {
                            generation,
                            stage,
                            transferred_bytes: 88_200_000,
                            total_bytes: 88_200_000,
                        },
                        start + Duration::from_secs(2),
                    ));
                    let content =
                        catalog_rows::build(&catalog, None, catalog_fixture_actions(), mtm);
                    let text = text_values(&content.view);
                    assert!(text.contains(&expected.into()));
                    assert!(!text.iter().any(|text| text.contains("/s")));
                    assert!(content.primary_labels.is_empty());
                }

                assert!(catalog.request_pause());
                let pausing = catalog_rows::build(&catalog, None, catalog_fixture_actions(), mtm);
                let pausing_text = text_values(&pausing.view);
                assert!(pausing_text.contains(&"Downloading model".into()));
                assert!(pausing_text.contains(&"Pausing transfer…".into()));
                assert!(!pausing_text.iter().any(|text| text.contains("/s")));
                assert!(pausing.primary_labels.is_empty());
                assert!(pausing.action_buttons.is_empty());
            }

            fn assert_installed_rows_and_priority(mtm: MainThreadMarker) {
                let mut installed = InstalledState::default();
                installed.replace(
                    [
                        "foxtrot", "alpha", "bravo", "charlie", "delta", "echo", "golf",
                    ]
                    .into_iter()
                    .map(|id| {
                        InstalledItem::new(
                            id.into(),
                            format!("{id}-q4.gguf"),
                            if id == "foxtrot" { 500_000 } else { 88_200_000 },
                        )
                    })
                    .collect(),
                    Some("foxtrot".into()),
                );
                assert!(installed.select("foxtrot"));
                installed.fail(InstalledInventoryError::RefreshFailed);
                let rows = super::installed_rows::build(
                    &installed,
                    None,
                    installed_fixture_actions(),
                    mtm,
                );
                assert_eq!(rows.view.frame().size.width, 360.0);
                assert_eq!(rows.row_buttons.len(), 5);
                assert_eq!(
                    rows.primary_labels[0].stringValue().to_string(),
                    "foxtrot-q4"
                );
                assert_eq!(
                    rows.secondary_labels[0].stringValue().to_string(),
                    "foxtrot · 500.0 KB"
                );
                assert_eq!(
                    rows.action_buttons
                        .iter()
                        .map(|button| button.title().to_string())
                        .collect::<Vec<_>>(),
                    ["Copy chat command", "Reveal in Finder"]
                );
                assert!(rows
                    .row_buttons
                    .iter()
                    .chain(rows.action_buttons.iter())
                    .all(|button| !button.refusesFirstResponder()));
                assert!(rows.row_buttons[0]
                    .accessibilityLabel()
                    .map(|label| label.to_string())
                    .is_some_and(|label| label.contains("selected; actions expanded")));
                let text = text_values(&rows.view);
                assert!(text.contains(&"2 more installed".into()));
                assert!(text.contains(&"Could not refresh installed models".into()));
                assert!(count_image_views(&rows.view) >= 6);

                let one = {
                    let mut state = InstalledState::default();
                    state.replace(
                        vec![InstalledItem::new(
                            "alpha".into(),
                            "alpha-q4.gguf".into(),
                            88_200_000,
                        )],
                        None,
                    );
                    state
                };
                let idle = CatalogState::default();
                let installed_menu = MenuRows::build(
                    &Fixture::Installed.snapshot(),
                    &idle,
                    &crate::menu::incomplete::IncompleteState::default(),
                    &one,
                    None,
                    layout_fixture_actions(),
                    mtm,
                );
                let installed_text = text_values(&installed_menu.view);
                assert!(installed_text.contains(&"alpha-q4".into()));
                assert!(!installed_text
                    .iter()
                    .any(|text| text.starts_with("Gemma 4 12B ·")));

                for (snapshot, expected) in [
                    (MenuSnapshot::loading(), "Loading Loxa status…"),
                    (Fixture::Invalid.snapshot(), "Managed bundle needs recovery"),
                    (Fixture::Paused.snapshot(), "Paused"),
                ] {
                    let content = MenuRows::build(
                        &snapshot,
                        &idle,
                        &crate::menu::incomplete::IncompleteState::default(),
                        &one,
                        None,
                        layout_fixture_actions(),
                        mtm,
                    );
                    let text = text_values(&content.view);
                    assert!(
                        text.iter().any(|text| text.starts_with(expected)),
                        "expected {expected:?} in {text:?}"
                    );
                    assert!(!text.contains(&"alpha-q4".into()));
                }

                let recommended = MenuRows::build(
                    &Fixture::Empty.snapshot(),
                    &idle,
                    &crate::menu::incomplete::IncompleteState::default(),
                    &one,
                    None,
                    layout_fixture_actions(),
                    mtm,
                );
                let recommended_text = text_values(&recommended.view);
                assert!(recommended_text.contains(&"alpha-q4".into()));
                assert!(recommended_text.contains(&"Recommended for this Mac".into()));

                let mut failed_catalog = CatalogState::default();
                let generation = failed_catalog
                    .submit_search("models")
                    .expect("the failure fixture starts active catalog work")
                    .generation();
                assert!(failed_catalog.apply(CatalogEvent::Failed {
                    generation,
                    message: "Catalog search failed".into(),
                }));
                for (snapshot, expected) in [
                    (MenuSnapshot::loading(), "Loading Loxa status…"),
                    (
                        MenuSnapshot::error("Core unavailable".into()),
                        "Core unavailable",
                    ),
                    (Fixture::Invalid.snapshot(), "Managed bundle needs recovery"),
                    (Fixture::Paused.snapshot(), "Paused"),
                ] {
                    let content = MenuRows::build(
                        &snapshot,
                        &failed_catalog,
                        &crate::menu::incomplete::IncompleteState::default(),
                        &one,
                        None,
                        layout_fixture_actions(),
                        mtm,
                    );
                    let text = text_values(&content.view);
                    assert!(text.contains(&"Catalog search failed".into()));
                    assert!(
                        text.iter().any(|text| text.starts_with(expected)),
                        "terminal catalog error hid {expected:?} in {text:?}"
                    );
                    assert!(!text.contains(&"alpha-q4".into()));
                }

                let mut active = CatalogState::default();
                assert!(active.submit_search("models").is_some());
                let browsing = MenuRows::build(
                    &MenuSnapshot::loading(),
                    &active,
                    &crate::menu::incomplete::IncompleteState::default(),
                    &one,
                    None,
                    layout_fixture_actions(),
                    mtm,
                );
                let browsing_text = text_values(&browsing.view);
                assert!(browsing_text.contains(&"Models".into()));
                assert!(!browsing_text.contains(&"Loading Loxa status…".into()));
                assert!(!browsing_text.contains(&"alpha-q4".into()));
            }

            fn assert_incomplete_rows_are_bounded_accessible_and_hidden_while_browsing(
                mtm: MainThreadMarker,
            ) {
                let mut incomplete = IncompleteState::default();
                incomplete.replace(vec![
                    IncompleteItem::new("delta".into(), 4_000_000, 10_000_000),
                    IncompleteItem::new("alpha".into(), 15_900_000, 88_200_000),
                    IncompleteItem::new("charlie".into(), 3_000_000, 10_000_000),
                    IncompleteItem::new("bravo".into(), 2_000_000, 10_000_000),
                ]);
                assert_eq!(incomplete.prepare_discard(0).as_deref(), Some("alpha"));
                let preparing = super::incomplete_rows::build(
                    &incomplete,
                    None,
                    super::incomplete_rows::IncompleteActions {
                        prepare: sel!(fixtureNoop:),
                        keep: sel!(fixtureNoop:),
                        confirm: sel!(fixtureNoop:),
                    },
                    mtm,
                );
                assert_eq!(preparing.action_buttons.len(), 3);
                assert!(preparing
                    .action_buttons
                    .iter()
                    .all(|button| !button.isEnabled()));
                assert_eq!(preparing.action_buttons[0].title().to_string(), "Checking…");
                assert_eq!(
                    preparing.action_buttons[0]
                        .accessibilityLabel()
                        .map(|label| label.to_string())
                        .as_deref(),
                    Some("Checking incomplete download alpha")
                );
                assert_eq!(
                    preparing.action_buttons[1]
                        .accessibilityLabel()
                        .map(|label| label.to_string())
                        .as_deref(),
                    Some("Prepare to discard the partial download for bravo")
                );
                assert!(incomplete.prepared("alpha", Ok(())));

                let rows = super::incomplete_rows::build(
                    &incomplete,
                    None,
                    super::incomplete_rows::IncompleteActions {
                        prepare: sel!(fixtureNoop:),
                        keep: sel!(fixtureNoop:),
                        confirm: sel!(fixtureNoop:),
                    },
                    mtm,
                );
                assert_eq!(rows.view.frame().size, NSSize::new(360.0, 234.0));
                assert_eq!(rows.action_buttons.len(), 5);
                assert!(rows.action_buttons.iter().all(|button| {
                    button
                        .accessibilityLabel()
                        .is_some_and(|label| !label.to_string().is_empty())
                }));
                let text = text_values(&rows.view);
                assert!(text.contains(&"alpha".into()));
                assert!(text.contains(&"18% · 15.9 MB of 88.2 MB".into()));
                assert!(text.contains(&"1 more — use loxa list".into()));
                assert!(!text.contains(&"delta".into()));

                assert_eq!(incomplete.confirm_discard().as_deref(), Some("alpha"));
                let discarding = super::incomplete_rows::build(
                    &incomplete,
                    None,
                    super::incomplete_rows::IncompleteActions {
                        prepare: sel!(fixtureNoop:),
                        keep: sel!(fixtureNoop:),
                        confirm: sel!(fixtureNoop:),
                    },
                    mtm,
                );
                assert_eq!(discarding.action_buttons.len(), 3);
                assert!(discarding
                    .action_buttons
                    .iter()
                    .all(|button| !button.isEnabled()));
                assert_eq!(
                    discarding.action_buttons[0].title().to_string(),
                    "Discarding…"
                );
                assert_eq!(
                    discarding.action_buttons[0]
                        .accessibilityLabel()
                        .map(|label| label.to_string())
                        .as_deref(),
                    Some("Discarding incomplete download alpha")
                );

                let menu = MenuRows::build(
                    &Fixture::Installed.snapshot(),
                    &CatalogState::default(),
                    &incomplete,
                    &InstalledState::default(),
                    None,
                    layout_fixture_actions(),
                    mtm,
                );
                assert!(visible_text_values(&menu.view).contains(&"Incomplete downloads".into()));

                let mut browsing = CatalogState::default();
                assert!(browsing.submit_search("models").is_some());
                let menu = MenuRows::build(
                    &Fixture::Installed.snapshot(),
                    &browsing,
                    &incomplete,
                    &InstalledState::default(),
                    None,
                    layout_fixture_actions(),
                    mtm,
                );
                let text = visible_text_values(&menu.view);
                assert!(!text.contains(&"Incomplete downloads".into()));
                assert!(!text.contains(&"alpha".into()));
            }

            fn assert_empty_inventory_error_is_rendered(mtm: MainThreadMarker) {
                let mut installed = InstalledState::default();
                installed.fail(InstalledInventoryError::RefreshFailed);
                let content = MenuRows::build(
                    &Fixture::Empty.snapshot(),
                    &CatalogState::default(),
                    &crate::menu::incomplete::IncompleteState::default(),
                    &installed,
                    None,
                    layout_fixture_actions(),
                    mtm,
                );
                let text = visible_text_values(&content.view);

                assert!(text.contains(&"Could not refresh installed models".into()));
                assert!(text.contains(&"Recommended for this Mac".into()));
            }

            fn assert_known_installed_row_outlives_cold_inventory_error(mtm: MainThreadMarker) {
                let mut installed = InstalledState::default();
                installed.fail(InstalledInventoryError::RefreshFailed);
                let content = MenuRows::build(
                    &Fixture::Installed.snapshot(),
                    &CatalogState::default(),
                    &crate::menu::incomplete::IncompleteState::default(),
                    &installed,
                    None,
                    layout_fixture_actions(),
                    mtm,
                );
                let text = visible_text_values(&content.view);

                assert!(
                    text.contains(&"Gemma 4 12B · Verified".into()),
                    "cold inventory failure hid the known installed bundle in {text:?}"
                );
                assert!(text.contains(&"Could not refresh installed models".into()));
            }

            fn layout_fixture_actions() -> Actions {
                Actions {
                    runtime_copy: sel!(fixtureNoop:),
                    search: sel!(fixtureNoop:),
                    repository: sel!(fixtureNoop:),
                    candidate: sel!(fixtureNoop:),
                    transfer: sel!(fixtureNoop:),
                    pause_transfer: sel!(fixtureNoop:),
                    installed_select: sel!(fixtureNoop:),
                    installed_copy: sel!(fixtureNoop:),
                    installed_reveal: sel!(fixtureNoop:),
                    incomplete_prepare: sel!(fixtureNoop:),
                    incomplete_keep: sel!(fixtureNoop:),
                    incomplete_confirm: sel!(fixtureNoop:),
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

            fn catalog_fixture_actions() -> catalog_rows::CatalogActions {
                catalog_rows::CatalogActions {
                    search: sel!(fixtureNoop:),
                    repository: sel!(fixtureNoop:),
                    candidate: sel!(fixtureNoop:),
                    transfer: sel!(fixtureNoop:),
                    pause: sel!(fixtureNoop:),
                }
            }

            fn installed_fixture_actions() -> super::installed_rows::InstalledActions {
                super::installed_rows::InstalledActions {
                    select: sel!(fixtureNoop:),
                    copy: sel!(fixtureNoop:),
                    reveal: sel!(fixtureNoop:),
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

            fn find_search_field(view: &NSView) -> Option<Retained<NSSearchField>> {
                for child in view.subviews() {
                    match child.downcast::<NSSearchField>() {
                        Ok(search) => return Some(search),
                        Err(child) => {
                            if let Some(search) = find_search_field(&child) {
                                return Some(search);
                            }
                        }
                    }
                }
                None
            }

            fn find_text_field(view: &NSView, text: &str) -> Option<Retained<NSTextField>> {
                let mut fields = Vec::new();
                collect_text_fields(view, &mut fields);
                fields
                    .into_iter()
                    .find(|field| field.stringValue().to_string() == text)
            }

            fn text_values(view: &NSView) -> Vec<String> {
                let mut fields = Vec::new();
                collect_text_fields(view, &mut fields);
                fields
                    .into_iter()
                    .map(|field| field.stringValue().to_string())
                    .collect()
            }

            pub(super) fn visible_text_values(view: &NSView) -> Vec<String> {
                let mut fields = Vec::new();
                collect_text_fields(view, &mut fields);
                fields
                    .into_iter()
                    .filter(|field| !field.isHidden())
                    .map(|field| field.stringValue().to_string())
                    .collect()
            }

            fn collect_text_fields(view: &NSView, fields: &mut Vec<Retained<NSTextField>>) {
                for child in view.subviews() {
                    match child.downcast::<NSTextField>() {
                        Ok(field) => fields.push(field),
                        Err(child) => collect_text_fields(&child, fields),
                    }
                }
            }

            fn count_image_views(view: &NSView) -> usize {
                use objc2_app_kit::NSImageView;

                view.subviews()
                    .into_iter()
                    .map(|child| match child.downcast::<NSImageView>() {
                        Ok(_) => 1,
                        Err(child) => count_image_views(&child),
                    })
                    .sum()
            }

            fn all_image_views(view: &NSView) -> Vec<Retained<objc2_app_kit::NSImageView>> {
                use objc2_app_kit::NSImageView;

                let mut images = Vec::new();
                for child in view.subviews() {
                    match child.downcast::<NSImageView>() {
                        Ok(image) => images.push(image),
                        Err(child) => images.extend(all_image_views(&child)),
                    }
                }
                images
            }
        }

        #[allow(clippy::items_after_test_module)] // The included source owns its unit-test module.
        pub(crate) mod controller {
            include!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/src/menu/macos/controller.rs"
            ));

            use std::cell::Cell;
            use std::time::Duration;

            use objc2::AnyThread as _;
            use objc2_app_kit::{NSProgressIndicator, NSView, NSWindow};
            use objc2_foundation::NSRange;

            struct SearchActionTargetIvars {
                borrow: RefCell<()>,
                calls: Cell<usize>,
                borrow_conflicts: Cell<usize>,
            }

            define_class!(
                #[unsafe(super = NSObject)]
                #[name = "LoxaSearchActionTargetFixture"]
                #[thread_kind = MainThreadOnly]
                #[ivars = SearchActionTargetIvars]
                struct SearchActionTarget;

                unsafe impl NSObjectProtocol for SearchActionTarget {}

                impl SearchActionTarget {
                    #[unsafe(method(delayedSearch:))]
                    fn delayed_search(&self, _sender: Option<&NSSearchField>) {
                        let ivars = self.ivars();
                        ivars.calls.set(ivars.calls.get() + 1);
                        if ivars.borrow.try_borrow_mut().is_err() {
                            ivars
                                .borrow_conflicts
                                .set(ivars.borrow_conflicts.get() + 1);
                        }
                    }
                }
            );

            impl SearchActionTarget {
                fn new(mtm: MainThreadMarker) -> Retained<Self> {
                    let this = Self::alloc(mtm).set_ivars(SearchActionTargetIvars {
                        borrow: RefCell::new(()),
                        calls: Cell::new(0),
                        borrow_conflicts: Cell::new(0),
                    });
                    // SAFETY: NSObject's init selector has the expected signature.
                    unsafe { msg_send![super(this), init] }
                }
            }

            struct ReplacementControllerIvars {
                outgoing_search: RefCell<Option<Retained<NSSearchField>>>,
            }

            define_class!(
                #[unsafe(super(NSViewController))]
                #[name = "LoxaReplacementControllerFixture"]
                #[thread_kind = MainThreadOnly]
                #[ivars = ReplacementControllerIvars]
                struct ReplacementController;

                unsafe impl NSObjectProtocol for ReplacementController {}

                impl ReplacementController {
                    #[unsafe(method(setView:))]
                    fn set_view(&self, view: &NSView) {
                        if let Some(search) = self.ivars().outgoing_search.borrow().as_ref() {
                            let action = search.action();
                            let target = search.target();
                            // SAFETY: the fixture only forwards the exact target/action pair
                            // installed by the product search field.
                            unsafe {
                                search.sendAction_to(action, target.as_deref());
                            }
                        }
                        // SAFETY: NSViewController implements setView: with this exact ABI.
                        unsafe { msg_send![super(self), setView: view] }
                    }
                }
            );

            impl ReplacementController {
                fn new(mtm: MainThreadMarker) -> Retained<Self> {
                    let this = Self::alloc(mtm).set_ivars(ReplacementControllerIvars {
                        outgoing_search: RefCell::new(None),
                    });
                    // SAFETY: NSViewController's init selector has the expected signature.
                    unsafe { msg_send![super(this), init] }
                }

                fn arm_delayed_action(&self, search: Retained<NSSearchField>) {
                    *self.ivars().outgoing_search.borrow_mut() = Some(search);
                }
            }

            pub(crate) fn assert_feedback_close_rebuild_contract(mtm: MainThreadMarker) {
                // SAFETY: this inert status item is only retained to satisfy the
                // controller state shape; the native geometry test never displays it.
                let status_item: Retained<NSStatusItem> =
                    unsafe { msg_send![NSStatusItem::alloc(), init] };
                let popover = NSPopover::new(mtm);
                let content_view_controller = NSViewController::new(mtm);
                popover.setContentViewController(Some(&content_view_controller));
                let mut state = NativePopoverState::new(
                    status_item,
                    popover.clone(),
                    content_view_controller.clone(),
                    Fixture::Installed,
                );
                state.installed.borrow_mut().replace(
                    vec![crate::menu::installed::InstalledItem::new(
                        "alpha".into(),
                        "alpha-q4.gguf".into(),
                        88_200_000,
                    )],
                    None,
                );
                assert!(state.installed.borrow_mut().select("alpha"));
                assert!(state.installed.borrow_mut().apply_feedback(
                    "alpha",
                    Some(crate::menu::installed::InstalledFeedback::ChatCommandCopied),
                ));
                let target = NSObject::new();
                state.render(&target, action_selectors(), mtm);

                let expanded_frame_height = content_view_controller.view().frame().size.height;
                let expanded_popover_height = popover.contentSize().height;
                assert_eq!(expanded_frame_height, expanded_popover_height);
                assert!(
                    super::rows::visible_text_values(&content_view_controller.view())
                        .contains(&"Chat command copied".into())
                );

                state.popover_closed(&target, action_selectors(), mtm);
                state.popover_opened();

                let compact_frame_height = content_view_controller.view().frame().size.height;
                let compact_popover_height = popover.contentSize().height;
                assert_eq!(expanded_frame_height - compact_frame_height, 24.0);
                assert_eq!(compact_frame_height, compact_popover_height);
                assert_eq!(state.installed.borrow().feedback_message(), None);
                assert!(
                    !super::rows::visible_text_values(&content_view_controller.view())
                        .contains(&"Chat command copied".into())
                );
            }

            pub(crate) fn assert_rebuild_detaches_outgoing_search_action(mtm: MainThreadMarker) {
                let target = SearchActionTarget::new(mtm);
                let controller = ReplacementController::new(mtm);
                let controller_base = controller.clone().into_super();
                let mut state = native_state(controller_base, Fixture::Installed, mtm);
                let mut actions = action_selectors();
                actions.search = sel!(delayedSearch:);
                state.render(&target, actions, mtm);
                let outgoing = state
                    .rows
                    .as_ref()
                    .expect("the initial render retains its rows")
                    .search_field();
                controller.arm_delayed_action(outgoing);

                let generation = state
                    .catalog
                    .submit_search("bartowski")
                    .expect("a valid query forces a result-layout rebuild")
                    .generation();
                assert_eq!(generation, 1);
                let _borrow = target.ivars().borrow.borrow_mut();
                state.render(&target, actions, mtm);

                assert_eq!(
                    target.ivars().calls.get(),
                    0,
                    "replacing an outgoing search field must not deliver its delayed action"
                );
                assert_eq!(
                    target.ivars().borrow_conflicts.get(),
                    0,
                    "replacement must not re-enter an already borrowed menu target"
                );
                let replacement = state
                    .rows
                    .as_ref()
                    .expect("the replacement retains its rows")
                    .search_field();
                assert_eq!(replacement.action(), Some(sel!(delayedSearch:)));
                assert!(replacement.target().is_some_and(|installed| {
                    Retained::as_ptr(&installed).cast::<()>()
                        == Retained::as_ptr(&target).cast::<()>()
                }));
            }

            pub(crate) fn assert_progress_updates_retain_active_search(mtm: MainThreadMarker) {
                let controller = NSViewController::new(mtm);
                let mut state = native_state(controller.clone(), Fixture::Installed, mtm);
                let (catalog, progress_start) = transferring_catalog();
                state.catalog = catalog;
                let target = NSObject::new();
                state.render(&target, action_selectors(), mtm);

                let window = unsafe { NSWindow::init(NSWindow::alloc(mtm)) };
                window.setContentView(Some(&controller.view()));
                let before_search = state
                    .rows
                    .as_ref()
                    .expect("the first progress render retains its rows")
                    .search_field();
                assert!(window.makeFirstResponder(Some(&before_search)));
                before_search.setStringValue(&NSString::from_str("draft search text"));
                let before_editor = before_search
                    .currentEditor()
                    .expect("the active search owns a field editor");
                let selected = NSRange::new(3, 6);
                before_editor.setSelectedRange(selected);

                let generation = state.catalog.generation();
                assert!(state.catalog.apply_at(
                    crate::menu::catalog::CatalogEvent::Progress {
                        generation,
                        stage: crate::menu::catalog::TransferStage::Transferring,
                        transferred_bytes: 25_900_000,
                        total_bytes: 88_200_000,
                    },
                    progress_start + Duration::from_secs(2),
                ));
                state.render(&target, action_selectors(), mtm);

                let after_search = state
                    .rows
                    .as_ref()
                    .expect("the updated progress render retains its rows")
                    .search_field();
                let after_editor = after_search
                    .currentEditor()
                    .expect("the retained search keeps its field editor");
                assert!(
                    std::ptr::eq(&*before_search, &*after_search),
                    "same-shape progress must retain the exact NSSearchField"
                );
                assert!(
                    std::ptr::eq(&*before_editor, &*after_editor),
                    "same-shape progress must retain the exact field editor"
                );
                assert_eq!(after_search.stringValue().to_string(), "draft search text");
                assert_eq!(after_editor.selectedRange(), selected);
                assert_eq!(after_search.action(), Some(sel!(submitSearch:)));
                assert!(after_search.target().is_some_and(|installed| {
                    Retained::as_ptr(&installed).cast::<()>()
                        == Retained::as_ptr(&target).cast::<()>()
                }));

                let view = controller.view();
                let visible = super::rows::visible_text_values(&view);
                assert!(visible.contains(&"29% · 25.9 MB of 88.2 MB".into()));
                assert!(
                    visible.contains(&"5.0 MB/s · 13s remaining".into()),
                    "visible transfer text: {visible:?}"
                );
                assert_eq!(
                    progress_value(&view),
                    Some(25_900_000.0 / 88_200_000.0),
                    "the retained progress indicator must receive the latest fraction"
                );
            }

            fn native_state(
                content_view_controller: Retained<NSViewController>,
                fixture: Fixture,
                mtm: MainThreadMarker,
            ) -> NativePopoverState {
                // SAFETY: this inert status item is only retained to satisfy the
                // controller state shape; the fixture never displays it.
                let status_item: Retained<NSStatusItem> =
                    unsafe { msg_send![NSStatusItem::alloc(), init] };
                let popover = NSPopover::new(mtm);
                popover.setContentViewController(Some(&content_view_controller));
                NativePopoverState::new(status_item, popover, content_view_controller, fixture)
            }

            fn transferring_catalog() -> (crate::menu::catalog::CatalogState, Instant) {
                use crate::menu::catalog::{CandidateItem, CatalogEvent, RepositoryItem};

                let mut catalog = crate::menu::catalog::CatalogState::default();
                let generation = catalog.submit_search("models").unwrap().generation();
                assert!(catalog.apply(CatalogEvent::Repositories {
                    generation,
                    repositories: vec![RepositoryItem::new("owner/model".into(), None)],
                }));
                let inspect = catalog.inspect_repository(0).unwrap();
                assert!(catalog.apply(CatalogEvent::Candidates {
                    generation: inspect.generation(),
                    repo: "owner/model".into(),
                    revision: "0123456789abcdef0123456789abcdef01234567".into(),
                    candidates: vec![CandidateItem::new("model-q4.gguf".into(), Some(88_200_000),)],
                }));
                assert!(catalog.select_candidate(0));
                assert!(catalog.start_transfer().is_some());
                let progress_start = Instant::now();
                assert!(catalog.apply_at(
                    CatalogEvent::Progress {
                        generation,
                        stage: crate::menu::catalog::TransferStage::Transferring,
                        transferred_bytes: 15_900_000,
                        total_bytes: 88_200_000,
                    },
                    progress_start,
                ));
                assert!(catalog.apply_at(
                    CatalogEvent::Progress {
                        generation,
                        stage: crate::menu::catalog::TransferStage::Transferring,
                        transferred_bytes: 20_900_000,
                        total_bytes: 88_200_000,
                    },
                    progress_start + Duration::from_secs(1),
                ));
                (catalog, progress_start)
            }

            fn progress_value(view: &NSView) -> Option<f64> {
                for child in view.subviews() {
                    match child.downcast::<NSProgressIndicator>() {
                        Ok(progress) => return Some(progress.doubleValue()),
                        Err(child) => {
                            if let Some(value) = progress_value(&child) {
                                return Some(value);
                            }
                        }
                    }
                }
                None
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
    menu::macos::controller::assert_progress_updates_retain_active_search(mtm);
    menu::macos::controller::assert_rebuild_detaches_outgoing_search_action(mtm);
    menu::macos::assert_native_timer_contract(mtm);
}

#[cfg(not(target_os = "macos"))]
fn main() {}
