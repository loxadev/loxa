use super::*;

#[test]
fn filter_precedence_uses_the_highest_priority_present_filter() {
    let filter = selected_filter(|name| match name {
        "LOXA_LOG" => Some("loxa=debug".into()),
        "RUST_LOG" => Some("loxa=trace".into()),
        _ => None,
    })
    .expect("valid highest-priority filter");
    assert_eq!(filter.to_string(), "loxa=debug");

    let filter = selected_filter(|name| (name == "RUST_LOG").then_some("loxa=trace".into()))
        .expect("valid RUST_LOG filter");
    assert_eq!(filter.to_string(), "loxa=trace");

    let filter = selected_filter(|_| None).expect("default filter");
    assert_eq!(filter.to_string(), DEFAULT_FILTER);
}

#[test]
fn invalid_selected_filter_fails_without_falling_through() {
    let error = selected_filter(|name| match name {
        "LOXA_LOG" => Some("not a[filter".into()),
        "RUST_LOG" => Some("loxa=trace".into()),
        _ => None,
    })
    .unwrap_err();

    assert_eq!(error, "LOXA_LOG");

    let error =
        selected_filter(|name| (name == "RUST_LOG").then_some("not a[filter".into())).unwrap_err();

    assert_eq!(error, "RUST_LOG");
}

#[test]
fn diagnostics_worker_constructor_panics_become_setup_errors() {
    let error = nonfatal_constructor::<()>(|| panic!("synthetic constructor failure")).unwrap_err();

    assert_eq!(error, "failed to initialize diagnostics worker");
}

#[test]
fn incomplete_drain_is_reported_as_unhealthy() {
    let health = SinkHealth::default();
    health.record_incomplete_drain();

    let snapshot = health.snapshot(0);
    assert!(snapshot.drain_incomplete);
    assert!(!snapshot.is_healthy());
}
