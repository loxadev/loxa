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

#[cfg(unix)]
fn active_command_with_umask(mask: libc::mode_t) -> Command {
    use std::os::unix::process::CommandExt;

    let mut command = Command::new(env!("CARGO_BIN_EXE_loxa"));
    command
        .args(["rm", "missing", "--yes"])
        .env_remove("LOXA_LOG")
        .env_remove("RUST_LOG");
    // SAFETY: umask is async-signal-safe and only changes the child process;
    // this closure captures no state besides the requested mask.
    unsafe {
        command.pre_exec(move || {
            libc::umask(mask);
            Ok(())
        });
    }
    command
}

#[cfg(unix)]
fn assert_directory_mode(path: &std::path::Path, expected: u32) {
    use std::os::unix::fs::PermissionsExt;

    assert_eq!(
        fs::metadata(path)
            .expect("directory metadata")
            .permissions()
            .mode()
            & 0o777,
        expected,
        "{}",
        path.display()
    );
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

#[cfg(unix)]
#[test]
fn active_command_creates_a_private_default_home() {
    let home = tempdir().expect("temporary user home");
    let root = home.path().join(".loxa");
    assert!(!root.exists());

    let output = active_command_with_umask(0o022)
        .env("HOME", home.path())
        .env_remove("LOXA_HOME")
        .env_remove("USERPROFILE")
        .output()
        .expect("run active loxa command with default home");

    assert!(!output.status.success(), "{output:?}");
    assert!(log_events(&root)
        .iter()
        .any(|event| event["event"] == "cli_startup"));
    for directory in [&root, &root.join("logs"), &root.join("logs/cli")] {
        assert_directory_mode(directory, 0o700);
    }
}

#[cfg(unix)]
#[test]
fn active_command_creates_private_nested_home_directories_under_permissive_umasks() {
    use std::os::unix::fs::PermissionsExt;

    for mask in [0o022, 0o000] {
        let ancestor = tempdir().expect("temporary existing ancestor");
        fs::set_permissions(ancestor.path(), fs::Permissions::from_mode(0o755))
            .expect("set existing ancestor permissions");
        let parent = ancestor.path().join("nested");
        let root = parent.join("loxa-home");
        assert!(!parent.exists());

        let output = active_command_with_umask(mask)
            .env("LOXA_HOME", &root)
            .output()
            .expect("run active loxa command with nested home");

        assert!(!output.status.success(), "{output:?}");
        assert!(log_events(&root)
            .iter()
            .any(|event| event["event"] == "cli_startup"));
        for directory in [&parent, &root, &root.join("logs"), &root.join("logs/cli")] {
            assert_directory_mode(directory, 0o700);
        }
        assert_directory_mode(ancestor.path(), 0o755);
    }
}

#[cfg(unix)]
#[test]
fn active_command_preserves_an_existing_home_and_sentinel() {
    use std::os::unix::fs::PermissionsExt;

    let home = tempdir().expect("temporary existing Loxa home");
    fs::set_permissions(home.path(), fs::Permissions::from_mode(0o755))
        .expect("set existing home permissions");
    let sentinel = home.path().join("keep");
    fs::write(&sentinel, b"existing data").expect("create sentinel");
    fs::set_permissions(&sentinel, fs::Permissions::from_mode(0o644))
        .expect("set sentinel permissions");

    let output = active_command_with_umask(0o022)
        .env("LOXA_HOME", home.path())
        .output()
        .expect("run active loxa command with existing home");

    assert!(!output.status.success(), "{output:?}");
    assert!(log_events(home.path())
        .iter()
        .any(|event| event["event"] == "cli_startup"));
    assert_directory_mode(home.path(), 0o755);
    assert_eq!(
        fs::read(&sentinel).expect("read sentinel"),
        b"existing data"
    );
    assert_eq!(
        fs::metadata(&sentinel)
            .expect("sentinel metadata")
            .permissions()
            .mode()
            & 0o777,
        0o644
    );
    assert_directory_mode(&home.path().join("logs"), 0o700);
    assert_directory_mode(&home.path().join("logs/cli"), 0o700);
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
