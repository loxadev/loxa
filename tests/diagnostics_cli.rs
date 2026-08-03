use std::fs;
use std::process::Command;

use tempfile::tempdir;

fn log_events(home: &std::path::Path) -> Vec<serde_json::Value> {
    let entries = fs::read_dir(home.join("logs"))
        .expect("diagnostics log directory")
        .collect::<Result<Vec<_>, _>>()
        .expect("diagnostics log entries");
    assert_eq!(entries.len(), 1, "expected one daily diagnostics log");
    fs::read_to_string(entries[0].path())
        .expect("diagnostics events")
        .lines()
        .map(|line| serde_json::from_str(line).expect("JSONL event"))
        .collect()
}

#[test]
fn list_writes_a_structured_startup_event_to_a_private_daily_log() {
    let home = tempdir().expect("temporary Loxa home");
    let output = Command::new(env!("CARGO_BIN_EXE_loxa"))
        .arg("list")
        .env("LOXA_HOME", home.path())
        .env_remove("LOXA_LOG")
        .env_remove("RUST_LOG")
        .output()
        .expect("run loxa list");

    assert!(output.status.success(), "{output:?}");

    let logs = home.path().join("logs");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        assert_eq!(
            fs::metadata(&logs)
                .expect("diagnostics log metadata")
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
    }

    let events = log_events(home.path());
    let startup = events
        .iter()
        .find(|event| event["event"] == "cli_startup")
        .expect("structured startup event");
    assert_eq!(startup["command"], "list");
}

#[test]
fn invalid_primary_filter_is_recorded_even_when_fallback_disables_normal_events() {
    let home = tempdir().expect("temporary Loxa home");
    let output = Command::new(env!("CARGO_BIN_EXE_loxa"))
        .arg("list")
        .env("LOXA_HOME", home.path())
        .env("LOXA_LOG", "not a[filter")
        .env("RUST_LOG", "off")
        .output()
        .expect("run loxa list");

    assert!(output.status.success(), "{output:?}");
    let events = log_events(home.path());
    let warning = events
        .iter()
        .find(|event| event["event"] == "invalid_log_filter")
        .expect("invalid-filter warning");
    assert_eq!(warning["source"], "LOXA_LOG");
}
