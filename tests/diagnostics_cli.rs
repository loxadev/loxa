use std::fs;
use std::process::Command;

use tempfile::tempdir;

fn log_events(home: &std::path::Path) -> Vec<serde_json::Value> {
    let entries = fs::read_dir(home.join("logs/cli"))
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
fn passive_list_does_not_create_or_retain_diagnostics() {
    let home = tempdir().expect("temporary Loxa home");
    let output = Command::new(env!("CARGO_BIN_EXE_loxa"))
        .arg("list")
        .env("LOXA_HOME", home.path())
        .env_remove("LOXA_LOG")
        .env_remove("RUST_LOG")
        .output()
        .expect("run loxa list");

    assert!(output.status.success(), "{output:?}");
    assert!(!home.path().join("logs").exists());
}

#[test]
fn active_command_writes_a_bounded_structured_startup_event() {
    let home = tempdir().expect("temporary Loxa home");
    let output = Command::new(env!("CARGO_BIN_EXE_loxa"))
        .args(["rm", "missing", "--yes"])
        .env("LOXA_HOME", home.path())
        .env_remove("LOXA_LOG")
        .env_remove("RUST_LOG")
        .output()
        .expect("run active loxa command");

    assert!(!output.status.success(), "{output:?}");

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
    assert_eq!(startup["command"], "rm");
}

#[test]
fn diagnostics_setup_failure_is_nonfatal_to_the_command() {
    let home = tempdir().expect("temporary Loxa home");
    let logs = home.path().join("logs");
    fs::write(&logs, b"not a diagnostics directory").expect("create conflicting log path");

    let output = Command::new(env!("CARGO_BIN_EXE_loxa"))
        .args(["rm", "missing", "--yes"])
        .env("LOXA_HOME", home.path())
        .env_remove("LOXA_LOG")
        .env_remove("RUST_LOG")
        .output()
        .expect("run loxa list");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "{output:?}");
    assert!(stderr.contains("diagnostics are unavailable"), "{stderr}");
    assert!(stderr.contains("unknown model id missing"), "{stderr}");
    assert!(!stderr.contains(&logs.display().to_string()), "{stderr}");
}

#[test]
fn invalid_primary_filter_is_nonfatal_and_does_not_leak_its_value() {
    let home = tempdir().expect("temporary Loxa home");
    let output = Command::new(env!("CARGO_BIN_EXE_loxa"))
        .args(["rm", "missing", "--yes"])
        .env("LOXA_HOME", home.path())
        .env("LOXA_LOG", "not a[filter")
        .env("RUST_LOG", "off")
        .output()
        .expect("run loxa list");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "{output:?}");
    assert!(stderr.contains("diagnostics are unavailable"), "{stderr}");
    assert!(stderr.contains("unknown model id missing"), "{stderr}");
    assert!(!stderr.contains("not a[filter"), "{stderr}");
}

#[cfg(unix)]
#[test]
fn non_unicode_primary_filter_is_nonfatal_without_falling_through() {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;

    let home = tempdir().expect("temporary Loxa home");
    let output = Command::new(env!("CARGO_BIN_EXE_loxa"))
        .args(["rm", "missing", "--yes"])
        .env("LOXA_HOME", home.path())
        .env("LOXA_LOG", OsString::from_vec(vec![0xff]))
        .env("RUST_LOG", "loxa=trace")
        .output()
        .expect("run loxa list");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "{output:?}");
    assert!(stderr.contains("diagnostics are unavailable"), "{stderr}");
    assert!(stderr.contains("unknown model id missing"), "{stderr}");
}
