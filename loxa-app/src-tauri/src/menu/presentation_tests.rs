use crate::menu::presentation::{
    Bundle, Download, DownloadPhase, Fixture, InlineCancelState, MenuAction, MenuLayout,
    MenuSection, MenuSnapshot, MenuUpdate, Recommendation, RecommendationUnavailableReason,
    RecoveryReason, Runtime, RuntimeInventory,
};

const TARGET_BYTES: u64 = 6_716_356_800;
const DRAFT_BYTES: u64 = 253_708_800;

#[test]
fn recommendation_fixture_retains_its_quantization_metadata() {
    assert_eq!(
        Fixture::Empty
            .snapshot()
            .recommendation_row()
            .unwrap()
            .subtitle()
            .as_deref(),
        Some("12B · Q4_K_M · MTP · 7.0 GB")
    );
}

#[test]
fn installed_fixture_retains_its_quantization_metadata() {
    assert_eq!(
        Fixture::Installed
            .snapshot()
            .installed_row()
            .unwrap()
            .subtitle(),
        "Q4_K_M · MTP · 7.0 GB"
    );
}

#[test]
fn loading_and_setup_error_are_owned_menu_states_not_fixture_snapshots() {
    let loading = MenuSnapshot::loading();
    assert!(loading.is_loading());
    assert_eq!(loading.runtime_label(), "Loading");
    assert_eq!(loading.error_message(), None);

    let error = MenuSnapshot::error("set LOXA_HOME, HOME, or USERPROFILE".to_owned());
    assert!(!error.is_loading());
    assert_eq!(error.runtime_label(), "Runtime: Unavailable");
    assert_eq!(
        error.error_message(),
        Some("set LOXA_HOME, HOME, or USERPROFILE")
    );
}

#[test]
fn clean_absence_composes_a_recommended_bundle_with_a_start_action() {
    let snapshot = MenuSnapshot::new(
        Bundle::Absent,
        Recommendation::available(TARGET_BYTES, DRAFT_BYTES),
        Download::Idle,
        Runtime::Idle,
        RuntimeInventory::Missing,
    )
    .expect("a clean absence with an eligible recommendation is canonical");

    assert_eq!(
        snapshot.section_kinds(),
        vec![
            MenuSection::Header,
            MenuSection::InstalledEmpty,
            MenuSection::Recommendation,
            MenuSection::Footer,
        ]
    );

    let recommendation = snapshot
        .recommendation_row()
        .expect("clean absence renders its one recommendation row");
    assert_eq!(recommendation.action(), Some(MenuAction::Start));
    assert_eq!(
        recommendation.subtitle().as_deref(),
        Some("12B · MTP · 7.0 GB")
    );
    assert_eq!(
        recommendation.size_detail().as_deref(),
        Some("6,970,065,600 bytes")
    );
}

#[test]
fn visible_download_sizes_use_decimal_units_but_keep_exact_byte_details() {
    let total_bytes = TARGET_BYTES + DRAFT_BYTES;
    let recommended = MenuSnapshot::new(
        Bundle::Absent,
        Recommendation::available(TARGET_BYTES, DRAFT_BYTES),
        Download::Idle,
        Runtime::Idle,
        RuntimeInventory::Missing,
    )
    .expect("an eligible fixture is canonical");
    let recommendation = recommended.recommendation_row().unwrap();
    assert_eq!(
        recommendation.subtitle().as_deref(),
        Some("12B · MTP · 7.0 GB")
    );
    assert_eq!(
        recommendation.size_detail().as_deref(),
        Some("6,970,065,600 bytes")
    );

    let downloading = MenuSnapshot::new(
        Bundle::Partial,
        Recommendation::Hidden,
        Download::active(1_234_567_890, total_bytes, DownloadPhase::Target),
        Runtime::Idle,
        RuntimeInventory::Missing,
    )
    .expect("an active fixture is canonical");
    let transfer = downloading.transfer_row().unwrap();
    assert_eq!(transfer.progress_text(), "1.2 GB of 7.0 GB");
    assert_eq!(
        transfer.progress_detail(),
        "1,234,567,890 of 6,970,065,600 bytes"
    );
}

#[test]
fn verified_bundle_hides_recommendation_and_surfaces_active_runtime() {
    let snapshot = MenuSnapshot::new(
        Bundle::verified(TARGET_BYTES, DRAFT_BYTES),
        Recommendation::Hidden,
        Download::Idle,
        Runtime::Running,
        RuntimeInventory::External,
    )
    .expect("a verified bundle with an idle download is canonical");

    assert_eq!(
        snapshot.section_kinds(),
        vec![
            MenuSection::Header,
            MenuSection::InstalledModel,
            MenuSection::Footer,
        ]
    );

    let installed = snapshot
        .installed_row()
        .expect("the verified bundle has exactly one installed row");
    assert_eq!(installed.subtitle(), "MTP · 7.0 GB");
    assert_eq!(installed.size_detail(), "6,970,065,600 bytes");
    assert_eq!(installed.runtime_note(), Some("Active runtime"));
    assert_eq!(snapshot.footer().label("0.1.0"), "Loxa 0.1.0");
}

#[test]
fn installed_bundle_always_exposes_its_verified_status() {
    for (runtime, expected_runtime_note) in [
        (Runtime::Idle, None),
        (Runtime::Running, Some("Active runtime")),
    ] {
        let snapshot = MenuSnapshot::new(
            Bundle::verified(TARGET_BYTES, DRAFT_BYTES),
            Recommendation::Hidden,
            Download::Idle,
            runtime,
            RuntimeInventory::Managed,
        )
        .expect("a verified bundle is canonical while idle or running");
        let installed = snapshot
            .installed_row()
            .expect("verified bundle has one row");
        assert_eq!(installed.verification_label(), "Verified");
        assert_eq!(installed.runtime_note(), expected_runtime_note);
    }
}

#[test]
fn recovery_bundle_composes_one_recovery_row_without_a_recommendation() {
    for (reason, expected_detail) in [
        (
            RecoveryReason::Invalid,
            "Verify the managed bundle before trying again.",
        ),
        (
            RecoveryReason::Busy,
            "Another Loxa process is updating this bundle.",
        ),
    ] {
        let snapshot = MenuSnapshot::new(
            Bundle::recovery(reason),
            Recommendation::Hidden,
            Download::Idle,
            Runtime::Error,
            RuntimeInventory::Missing,
        )
        .expect("an invalid or busy bundle stays fail-closed");

        assert_eq!(
            snapshot.section_kinds(),
            vec![
                MenuSection::Header,
                MenuSection::Recovery,
                MenuSection::Footer,
            ]
        );
        assert_eq!(snapshot.recovery_row().unwrap().detail(), expected_detail);
        assert!(snapshot.recommendation_row().is_none());
    }
}

#[test]
fn clean_ineligible_absence_keeps_the_recommendation_visible_but_disabled() {
    for (reason, expected_detail) in [
        (
            RecommendationUnavailableReason::InsufficientMemory,
            "Insufficient memory",
        ),
        (
            RecommendationUnavailableReason::InsufficientDisk,
            "Insufficient disk space",
        ),
        (
            RecommendationUnavailableReason::Unavailable,
            "Unavailable on this Mac",
        ),
    ] {
        let snapshot = MenuSnapshot::new(
            Bundle::Absent,
            Recommendation::unavailable(reason, TARGET_BYTES, DRAFT_BYTES),
            Download::Idle,
            Runtime::Idle,
            RuntimeInventory::Missing,
        )
        .expect("a clean but ineligible absence is canonical");

        assert_eq!(
            snapshot.section_kinds(),
            vec![
                MenuSection::Header,
                MenuSection::InstalledEmpty,
                MenuSection::Recommendation,
                MenuSection::Footer,
            ]
        );
        let recommendation = snapshot.recommendation_row().unwrap();
        assert_eq!(recommendation.action(), None);
        assert_eq!(recommendation.disabled_reason(), Some(expected_detail));
        assert_eq!(
            recommendation.subtitle().as_deref(),
            Some("12B · MTP · 7.0 GB")
        );
        assert_eq!(
            recommendation.size_detail().as_deref(),
            Some("6,970,065,600 bytes")
        );
    }
}

#[test]
fn partial_download_fixtures_keep_exact_progress_and_offer_the_matching_action() {
    let total_bytes = TARGET_BYTES + DRAFT_BYTES;
    for (download, expected_action, expected_phase, expected_progress) in [
        (
            Download::active(1_234_567_890, total_bytes, DownloadPhase::Target),
            MenuAction::Pause,
            "Downloading target",
            "1.2 GB of 7.0 GB",
        ),
        (
            Download::fixture_paused(2_345_678_901, total_bytes, DownloadPhase::Draft),
            MenuAction::Resume,
            "Paused during MTP draft",
            "2.3 GB of 7.0 GB",
        ),
        (
            Download::failed(3_456_789_012, total_bytes, DownloadPhase::Verifying),
            MenuAction::Retry,
            "Verification failed",
            "3.5 GB of 7.0 GB",
        ),
    ] {
        let snapshot = MenuSnapshot::new(
            Bundle::Partial,
            Recommendation::Hidden,
            download,
            Runtime::Idle,
            RuntimeInventory::Missing,
        )
        .expect("a partial bundle always has one non-idle transfer state");

        assert_eq!(
            snapshot.section_kinds(),
            vec![
                MenuSection::Header,
                MenuSection::Downloading,
                MenuSection::Footer
            ]
        );
        let transfer = snapshot.transfer_row().unwrap();
        assert_eq!(transfer.primary_action(), expected_action);
        assert_eq!(transfer.phase_label(), expected_phase);
        assert_eq!(transfer.progress_text(), expected_progress);
        assert!(transfer
            .progress_detail()
            .ends_with("of 6,970,065,600 bytes"));
    }
}

#[test]
fn transfer_progress_fraction_uses_the_authoritative_exact_bytes() {
    let total_bytes = TARGET_BYTES + DRAFT_BYTES;
    let empty = MenuSnapshot::new(
        Bundle::Partial,
        Recommendation::Hidden,
        Download::active(0, total_bytes, DownloadPhase::Preparing),
        Runtime::Idle,
        RuntimeInventory::Missing,
    )
    .unwrap();
    let complete = MenuSnapshot::new(
        Bundle::Partial,
        Recommendation::Hidden,
        Download::active(total_bytes, total_bytes, DownloadPhase::Publishing),
        Runtime::Idle,
        RuntimeInventory::Missing,
    )
    .unwrap();

    assert_eq!(empty.transfer_row().unwrap().progress_fraction(), 0.0);
    assert_eq!(
        empty.transfer_row().unwrap().progress_text(),
        "0 B of 7.0 GB"
    );
    assert_eq!(complete.transfer_row().unwrap().progress_fraction(), 1.0);
}

#[test]
fn layout_clamps_row_width_and_preserves_the_native_content_geometry() {
    assert_eq!(MenuLayout::width_for(240.0), 300.0);
    assert_eq!(MenuLayout::width_for(344.0), 344.0);
    assert_eq!(MenuLayout::width_for(480.0), 400.0);
    assert_eq!(MenuLayout::content_width(300.0), 274.0);
    assert_eq!(MenuLayout::VERTICAL_PADDING, 4.0);
    assert_eq!(MenuLayout::HOVER_RADIUS, 6.0);
    assert_eq!(MenuLayout::model_row_height(), 40.0);
    assert_eq!(MenuLayout::transfer_row_height(), 116.0);
    assert_eq!(MenuLayout::icon_container(), 28.0);
    assert_eq!(MenuLayout::primary_font_size(), 13.0);
    assert_eq!(MenuLayout::secondary_font_size(), 11.0);
}

#[test]
fn inline_cancel_confirmation_exposes_keep_and_discard_then_resets() {
    let mut confirmation = InlineCancelState::default();

    assert_eq!(confirmation.visible_actions(), vec![MenuAction::Cancel]);
    confirmation.activate_cancel();
    assert_eq!(
        confirmation.visible_actions(),
        vec![MenuAction::KeepPartial, MenuAction::DiscardPartial]
    );
    confirmation.keep_partial();
    assert_eq!(confirmation.visible_actions(), vec![MenuAction::Cancel]);

    confirmation.activate_cancel();
    assert!(confirmation.discard_partial());
    assert_eq!(confirmation.visible_actions(), vec![MenuAction::Cancel]);

    confirmation.activate_cancel();
    confirmation.reset();
    assert_eq!(confirmation.visible_actions(), vec![MenuAction::Cancel]);
}

#[test]
fn inline_cancel_confirmation_labels_the_keep_partial_action_explicitly() {
    let mut confirmation = InlineCancelState::default();
    confirmation.activate_cancel();

    let keep_partial = confirmation
        .visible_actions()
        .into_iter()
        .find(|action| *action == MenuAction::KeepPartial)
        .expect("cancelling exposes a keep-partial action");
    assert_eq!(keep_partial.confirmation_label(), Some("Keep partial"));
}

#[test]
fn inline_cancel_actions_describe_the_download_operation_for_accessibility() {
    for (action, expected) in [
        (MenuAction::Cancel, "Cancel download"),
        (MenuAction::KeepPartial, "Keep partial download"),
        (MenuAction::DiscardPartial, "Discard partial download"),
    ] {
        assert_eq!(action.accessibility_label(), Some(expected));
    }
}

#[test]
fn completed_fixture_is_an_installed_snapshot() {
    let fixture = Fixture::parse("completed").expect("completed is an explicit static fixture");
    let snapshot = fixture.snapshot();

    assert_eq!(
        snapshot.section_kinds(),
        vec![
            MenuSection::Header,
            MenuSection::InstalledModel,
            MenuSection::Footer
        ]
    );
    assert_eq!(
        snapshot
            .installed_row()
            .expect("completed renders the installed row")
            .verification_label(),
        "Verified"
    );
}

#[test]
fn header_runtime_labels_are_safe_and_complete_for_each_runtime_state() {
    for (runtime, expected) in [
        (Runtime::Idle, "Runtime: Stopped"),
        (Runtime::Starting, "Runtime: Starting"),
        (Runtime::Running, "Runtime: Running"),
        (Runtime::Stopping, "Runtime: Stopping"),
        (Runtime::Error, "Runtime: Error"),
    ] {
        let snapshot = MenuSnapshot::new(
            Bundle::Absent,
            Recommendation::available(TARGET_BYTES, DRAFT_BYTES),
            Download::Idle,
            runtime,
            RuntimeInventory::Missing,
        )
        .unwrap();
        assert_eq!(snapshot.runtime_label(), expected);
    }
}

#[test]
fn progress_updates_retained_rows_while_action_or_section_changes_rebuild() {
    let total_bytes = TARGET_BYTES + DRAFT_BYTES;
    let active_at_start = MenuSnapshot::new(
        Bundle::Partial,
        Recommendation::Hidden,
        Download::active(1, total_bytes, DownloadPhase::Target),
        Runtime::Idle,
        RuntimeInventory::Missing,
    )
    .unwrap();
    let active_later = MenuSnapshot::new(
        Bundle::Partial,
        Recommendation::Hidden,
        Download::active(2, total_bytes, DownloadPhase::Draft),
        Runtime::Running,
        RuntimeInventory::External,
    )
    .unwrap();
    let paused = MenuSnapshot::new(
        Bundle::Partial,
        Recommendation::Hidden,
        Download::fixture_paused(2, total_bytes, DownloadPhase::Draft),
        Runtime::Running,
        RuntimeInventory::External,
    )
    .unwrap();
    let installed = MenuSnapshot::new(
        Bundle::verified(TARGET_BYTES, DRAFT_BYTES),
        Recommendation::Hidden,
        Download::Idle,
        Runtime::Running,
        RuntimeInventory::External,
    )
    .unwrap();

    assert_eq!(
        active_later.update_from(Some(&active_at_start)),
        MenuUpdate::UpdateRetainedRows
    );
    assert_eq!(paused.update_from(Some(&active_later)), MenuUpdate::Rebuild);
    assert_eq!(
        installed.update_from(Some(&active_later)),
        MenuUpdate::Rebuild
    );
}

#[test]
fn named_fixtures_cover_each_static_menu_composition_without_live_observation() {
    for (name, expected_sections) in [
        (
            "empty",
            vec![
                MenuSection::Header,
                MenuSection::InstalledEmpty,
                MenuSection::Recommendation,
                MenuSection::Footer,
            ],
        ),
        (
            "installed",
            vec![
                MenuSection::Header,
                MenuSection::InstalledModel,
                MenuSection::Footer,
            ],
        ),
        (
            "busy",
            vec![
                MenuSection::Header,
                MenuSection::Recovery,
                MenuSection::Footer,
            ],
        ),
        (
            "publishing",
            vec![
                MenuSection::Header,
                MenuSection::Downloading,
                MenuSection::Footer,
            ],
        ),
    ] {
        let fixture = Fixture::parse(name).expect("known fixture name");
        assert_eq!(fixture.snapshot().section_kinds(), expected_sections);
    }

    assert!(Fixture::parse("not-a-fixture").is_none());
}

#[test]
fn ineligible_fixture_names_keep_the_recommendation_visible_without_a_download_action() {
    for (name, expected_reason) in [
        ("low-memory", "Insufficient memory"),
        ("low-disk", "Insufficient disk space"),
        ("unavailable", "Unavailable on this Mac"),
    ] {
        let fixture = Fixture::parse(name).expect("known ineligible fixture name");
        let snapshot = fixture.snapshot();
        let recommendation = snapshot
            .recommendation_row()
            .expect("clean absence retains its recommendation row");
        assert_eq!(recommendation.action(), None);
        assert_eq!(recommendation.disabled_reason(), Some(expected_reason));
    }
}

#[test]
fn footer_is_product_version_only_not_a_fixture_server_claim() {
    for fixture in [Fixture::Empty, Fixture::Installed, Fixture::Running] {
        assert_eq!(fixture.snapshot().footer().label("0.1.0"), "Loxa 0.1.0");
    }
}

#[test]
fn fixture_actions_transition_only_the_injected_snapshot() {
    let active = Fixture::Empty
        .snapshot()
        .apply_fixture_action(MenuAction::Start)
        .expect("start changes only the static fixture to its active presentation");
    assert_eq!(
        active.transfer_row().unwrap().primary_action(),
        MenuAction::Pause
    );

    let paused = active
        .apply_fixture_action(MenuAction::Pause)
        .expect("pause changes only the static fixture to its paused presentation");
    assert_eq!(
        paused.transfer_row().unwrap().primary_action(),
        MenuAction::Resume
    );

    let resumed = paused
        .apply_fixture_action(MenuAction::Resume)
        .expect("resume changes only the static fixture to its active presentation");
    assert_eq!(
        resumed.transfer_row().unwrap().primary_action(),
        MenuAction::Pause
    );

    let retried = Fixture::Failed
        .snapshot()
        .apply_fixture_action(MenuAction::Retry)
        .expect("retry changes only the failed fixture to an active presentation");
    assert_eq!(
        retried.transfer_row().unwrap().primary_action(),
        MenuAction::Pause
    );
}

#[test]
fn every_named_fixture_maps_to_its_exact_static_presentation() {
    #[derive(Debug, Eq, PartialEq)]
    enum ExpectedBody {
        Recommendation(&'static str),
        Installed(&'static str),
        Recovery(&'static str),
        Transfer,
    }

    struct Expectation {
        name: &'static str,
        runtime: &'static str,
        body: ExpectedBody,
        phase: Option<&'static str>,
        action: Option<MenuAction>,
        progress: Option<&'static str>,
    }

    let expectations = [
        Expectation {
            name: "empty",
            runtime: "Runtime: Stopped",
            body: ExpectedBody::Recommendation("Ready to download"),
            phase: None,
            action: Some(MenuAction::Start),
            progress: None,
        },
        Expectation {
            name: "low-memory",
            runtime: "Runtime: Stopped",
            body: ExpectedBody::Recommendation("Insufficient memory"),
            phase: None,
            action: None,
            progress: None,
        },
        Expectation {
            name: "low-disk",
            runtime: "Runtime: Stopped",
            body: ExpectedBody::Recommendation("Insufficient disk space"),
            phase: None,
            action: None,
            progress: None,
        },
        Expectation {
            name: "unavailable",
            runtime: "Runtime: Stopped",
            body: ExpectedBody::Recommendation("Unavailable on this Mac"),
            phase: None,
            action: None,
            progress: None,
        },
        Expectation {
            name: "installed",
            runtime: "Runtime: Stopped",
            body: ExpectedBody::Installed("Verified"),
            phase: None,
            action: None,
            progress: None,
        },
        Expectation {
            name: "completed",
            runtime: "Runtime: Stopped",
            body: ExpectedBody::Installed("Verified"),
            phase: None,
            action: None,
            progress: None,
        },
        Expectation {
            name: "running",
            runtime: "Runtime: Running",
            body: ExpectedBody::Installed("Verified"),
            phase: None,
            action: None,
            progress: None,
        },
        Expectation {
            name: "invalid",
            runtime: "Runtime: Error",
            body: ExpectedBody::Recovery("Verify the managed bundle before trying again."),
            phase: None,
            action: None,
            progress: None,
        },
        Expectation {
            name: "busy",
            runtime: "Runtime: Stopped",
            body: ExpectedBody::Recovery("Another Loxa process is updating this bundle."),
            phase: None,
            action: None,
            progress: None,
        },
        Expectation {
            name: "preparing",
            runtime: "Runtime: Stopped",
            body: ExpectedBody::Transfer,
            phase: Some("Preparing"),
            action: Some(MenuAction::Pause),
            progress: Some("0 of 6,970,065,600 bytes"),
        },
        Expectation {
            name: "downloading",
            runtime: "Runtime: Stopped",
            body: ExpectedBody::Transfer,
            phase: Some("Downloading target"),
            action: Some(MenuAction::Pause),
            progress: Some("1,234,567,890 of 6,970,065,600 bytes"),
        },
        Expectation {
            name: "draft",
            runtime: "Runtime: Stopped",
            body: ExpectedBody::Transfer,
            phase: Some("Downloading MTP draft"),
            action: Some(MenuAction::Pause),
            progress: Some("6,800,000,000 of 6,970,065,600 bytes"),
        },
        Expectation {
            name: "verifying",
            runtime: "Runtime: Stopped",
            body: ExpectedBody::Transfer,
            phase: Some("Verifying"),
            action: Some(MenuAction::Pause),
            progress: Some("6,970,065,600 of 6,970,065,600 bytes"),
        },
        Expectation {
            name: "publishing",
            runtime: "Runtime: Stopped",
            body: ExpectedBody::Transfer,
            phase: Some("Publishing"),
            action: Some(MenuAction::Pause),
            progress: Some("6,970,065,600 of 6,970,065,600 bytes"),
        },
        Expectation {
            name: "paused",
            runtime: "Runtime: Stopped",
            body: ExpectedBody::Transfer,
            phase: Some("Paused during MTP draft"),
            action: Some(MenuAction::Resume),
            progress: Some("2,345,678,901 of 6,970,065,600 bytes"),
        },
        Expectation {
            name: "failed",
            runtime: "Runtime: Stopped",
            body: ExpectedBody::Transfer,
            phase: Some("Verification failed"),
            action: Some(MenuAction::Retry),
            progress: Some("3,456,789,012 of 6,970,065,600 bytes"),
        },
        Expectation {
            name: "starting",
            runtime: "Runtime: Starting",
            body: ExpectedBody::Recommendation("Ready to download"),
            phase: None,
            action: Some(MenuAction::Start),
            progress: None,
        },
        Expectation {
            name: "stopping",
            runtime: "Runtime: Stopping",
            body: ExpectedBody::Recommendation("Ready to download"),
            phase: None,
            action: Some(MenuAction::Start),
            progress: None,
        },
        Expectation {
            name: "error",
            runtime: "Runtime: Error",
            body: ExpectedBody::Recommendation("Ready to download"),
            phase: None,
            action: Some(MenuAction::Start),
            progress: None,
        },
    ];

    for expected in expectations {
        let fixture = Fixture::parse(expected.name).expect("table contains a named fixture");
        let snapshot = fixture.snapshot();
        let (body, phase, action, progress) = if let Some(row) = snapshot.recommendation_row() {
            (
                ExpectedBody::Recommendation(row.disabled_reason().unwrap_or("Ready to download")),
                None,
                row.action(),
                None,
            )
        } else if let Some(row) = snapshot.installed_row() {
            (
                ExpectedBody::Installed(row.verification_label()),
                None,
                None,
                None,
            )
        } else if let Some(row) = snapshot.recovery_row() {
            (ExpectedBody::Recovery(row.detail()), None, None, None)
        } else if let Some(row) = snapshot.transfer_row() {
            (
                ExpectedBody::Transfer,
                Some(row.phase_label()),
                Some(row.primary_action()),
                Some(row.progress_detail()),
            )
        } else {
            panic!("{} has no menu body", expected.name);
        };

        assert_eq!(
            snapshot.runtime_label(),
            expected.runtime,
            "{}",
            expected.name
        );
        assert_eq!(body, expected.body, "{}", expected.name);
        assert_eq!(phase, expected.phase, "{}", expected.name);
        assert_eq!(action, expected.action, "{}", expected.name);
        assert_eq!(progress.as_deref(), expected.progress, "{}", expected.name);
    }
}
