use std::fs;
use std::path::Path;
use std::process::Command;

use serde_json::Value;
use tempfile::tempdir;

const OWNERSHIP_EVENT: &str = "log_initialized";

#[test]
fn inspect_help_explains_branch_tag_resolution_without_creating_state() {
    let root = tempdir().expect("temporary process environment");
    let loxa_home = root.path().join("loxa-home");
    let output = Command::new(env!("CARGO_BIN_EXE_loxa"))
        .args(["inspect", "--help"])
        .env("LOXA_HOME", &loxa_home)
        .env("NO_COLOR", "1")
        .output()
        .expect("run loxa inspect help");

    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_eq!(output.stderr, b"");
    let stdout = String::from_utf8(output.stdout).expect("UTF-8 help");
    assert!(
        stdout.contains("branches/tags resolve again when pulled"),
        "{stdout}"
    );
    assert!(!loxa_home.exists(), "help initialized local state");
}

fn owned_daily_log_events(home: &Path) -> Vec<Value> {
    let log_dir = home.join("logs");
    let mut entries = fs::read_dir(&log_dir)
        .expect("diagnostics log directory")
        .collect::<Result<Vec<_>, _>>()
        .expect("diagnostics log entries");
    entries.sort_by_key(|entry| entry.file_name());
    assert!(!entries.is_empty(), "expected at least one daily log");

    entries
        .into_iter()
        .flat_map(|entry| {
            let name = entry.file_name();
            let name = name.to_str().expect("UTF-8 diagnostics filename");
            assert!(name.starts_with("loxa.jsonl."), "unexpected log {name}");
            let contents = fs::read_to_string(entry.path()).expect("diagnostics events");
            let events = contents
                .lines()
                .map(|line| serde_json::from_str::<Value>(line).expect("JSONL event"))
                .collect::<Vec<_>>();
            assert_eq!(events[0]["event"], OWNERSHIP_EVENT, "unowned log {name}");
            assert_eq!(events[0]["application"], "loxa", "unowned log {name}");
            events
        })
        .collect()
}

fn assert_no_sensitive_log_fields(value: &Value) {
    match value {
        Value::Object(fields) => {
            for (key, child) in fields {
                let key = key.to_ascii_lowercase();
                for excluded in [
                    "query",
                    "repo",
                    "revision",
                    "candidate",
                    "url",
                    "body",
                    "token",
                ] {
                    assert!(
                        !key.contains(excluded),
                        "sensitive diagnostics field {key}: {value}"
                    );
                }
                assert_no_sensitive_log_fields(child);
            }
        }
        Value::Array(values) => {
            for child in values {
                assert_no_sensitive_log_fields(child);
            }
        }
        _ => {}
    }
}

fn assert_only_normal_logs(home: &Path) {
    let mut entries = fs::read_dir(home)
        .expect("Loxa home")
        .map(|entry| entry.expect("Loxa home entry").file_name())
        .collect::<Vec<_>>();
    entries.sort();
    assert_eq!(entries, ["logs"]);
}

#[test]
fn search_cli_prints_copyable_inspect_commands_and_logs_only_command_name() {
    let home = tempdir().expect("temporary Loxa home");
    let token_marker = "UNIQUE_DISCOVERY_TOKEN_URL_BODY_MARKER";
    let output = Command::new(env!("CARGO_BIN_EXE_loxa"))
        .args(["search", "hf://owner/repo"])
        .env("LOXA_HOME", home.path())
        .env_remove("LOXA_LOG")
        .env_remove("RUST_LOG")
        .env_remove("CLICOLOR_FORCE")
        .env_remove("FORCE_COLOR")
        .env_remove("HF_TOKEN_PATH")
        .env_remove("HF_HUB_DISABLE_IMPLICIT_TOKEN")
        .env("NO_COLOR", "1")
        .env("HF_TOKEN", token_marker)
        .output()
        .expect("run loxa search");

    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_eq!(
        String::from_utf8(output.stdout).expect("UTF-8 stdout"),
        concat!(
            "Repositories (1)\n",
            "\n",
            "owner/repo\n",
            "  Access: Unknown\n",
            "  Downloads: Unknown\n",
            "  Inspect: loxa inspect owner/repo\n",
        )
    );
    assert_eq!(output.stderr, b"");

    let events = owned_daily_log_events(home.path());
    let startup = events
        .iter()
        .find(|event| event["event"] == "cli_startup")
        .expect("startup event");
    let finish = events
        .iter()
        .find(|event| event["event"] == "cli_finished")
        .expect("finish event");
    assert_eq!(startup["command"], "search");
    assert_eq!(finish["command"], "search");
    assert_eq!(finish["exit_code"], 0);

    for event in &events {
        assert_no_sensitive_log_fields(event);
    }
    let logs = serde_json::to_string(&events).expect("serialize events");
    for secret in [
        "hf://owner/repo",
        "owner/repo",
        token_marker,
        "UNIQUE_DISCOVERY_TOKEN",
        "URL_BODY_MARKER",
    ] {
        assert!(
            !logs.contains(secret),
            "diagnostics leaked {secret}: {logs}"
        );
    }
    assert_only_normal_logs(home.path());
}

#[test]
fn invalid_inspect_prints_static_error_and_logs_only_command_name() {
    let home = tempdir().expect("temporary Loxa home");
    let raw_revision =
        "UNIQUE_RAW_REVISION\nhttps://evil.example/UNIQUE_RESPONSE_BODY?token=UNIQUE_URL_TOKEN";
    let token_marker = "UNIQUE_INSPECT_ENV_TOKEN";
    let output = Command::new(env!("CARGO_BIN_EXE_loxa"))
        .args(["inspect", "owner/repo", "--revision", raw_revision])
        .env("LOXA_HOME", home.path())
        .env_remove("LOXA_LOG")
        .env_remove("RUST_LOG")
        .env_remove("CLICOLOR_FORCE")
        .env_remove("FORCE_COLOR")
        .env_remove("HF_TOKEN_PATH")
        .env_remove("HF_HUB_DISABLE_IMPLICIT_TOKEN")
        .env("NO_COLOR", "1")
        .env("HF_TOKEN", token_marker)
        .output()
        .expect("run loxa inspect");

    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert_eq!(output.stdout, b"");
    let stderr = String::from_utf8(output.stderr).expect("UTF-8 stderr");
    assert_eq!(
        stderr,
        format!(
            "Error: invalid Hugging Face revision\nDiagnostics: {}\n",
            home.path().join("logs").display()
        )
    );
    for character in stderr.chars() {
        assert!(
            !character.is_control() || character == '\n',
            "unexpected control character in {stderr:?}"
        );
        assert!(
            !matches!(character, '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}'),
            "unexpected bidi control in {stderr:?}"
        );
    }
    for secret in [
        "\u{1b}",
        "owner/repo",
        "UNIQUE_RAW_REVISION",
        "evil.example",
        "UNIQUE_RESPONSE_BODY",
        "UNIQUE_URL_TOKEN",
        token_marker,
    ] {
        assert!(
            !stderr.contains(secret),
            "stderr leaked {secret}: {stderr:?}"
        );
    }

    let events = owned_daily_log_events(home.path());
    let startup = events
        .iter()
        .find(|event| event["event"] == "cli_startup")
        .expect("startup event");
    let failure = events
        .iter()
        .find(|event| event["event"] == "cli_failed")
        .expect("failure event");
    assert_eq!(startup["command"], "inspect");
    assert_eq!(failure["command"], "inspect");
    assert!(!events.iter().any(|event| event["event"] == "cli_finished"));

    for event in &events {
        assert_no_sensitive_log_fields(event);
    }
    let logs = serde_json::to_string(&events).expect("serialize events");
    for secret in [
        "owner/repo",
        "UNIQUE_RAW_REVISION",
        "evil.example",
        "UNIQUE_RESPONSE_BODY",
        "UNIQUE_URL_TOKEN",
        token_marker,
    ] {
        assert!(
            !logs.contains(secret),
            "diagnostics leaked {secret}: {logs}"
        );
    }
    assert_only_normal_logs(home.path());
}
