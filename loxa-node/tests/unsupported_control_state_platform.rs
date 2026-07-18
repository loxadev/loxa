#![cfg(all(
    not(any(target_os = "macos", target_os = "linux")),
    feature = "unsupported-platform-ci"
))]

use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn production_preflight_rejects_unsupported_platform_before_filesystem_mutation() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("test clock must follow the Unix epoch")
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "loxa-control-state-production-unsupported-{}-{nonce}",
        std::process::id()
    ));

    let error = loxa_node::unsupported_control_state_preflight_for_ci(
        &root.join("state/control-state.sqlite3"),
    )
    .expect_err("the production preflight must reject an unsupported platform");

    assert_eq!(error, "unsupported_platform");
    assert!(!root.exists());
}
