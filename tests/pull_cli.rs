use std::path::Path;
use std::process::Command;

use tempfile::tempdir;

fn controlled_command(root: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_loxa"));
    command
        .env("LOXA_HOME", root.join("loxa-home"))
        .env("HOME", root.join("home"))
        .env("USERPROFILE", root.join("userprofile"))
        .env("HF_HUB_DISABLE_IMPLICIT_TOKEN", "1")
        .env("NO_COLOR", "1");
    for name in [
        "LOXA_LOG",
        "RUST_LOG",
        "CLICOLOR_FORCE",
        "FORCE_COLOR",
        "HF_TOKEN",
        "HF_TOKEN_PATH",
        "HF_HOME",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "NO_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "no_proxy",
    ] {
        command.env_remove(name);
    }
    command
}

#[test]
fn selectorless_pull_fails_before_local_or_remote_work() {
    let temp = tempdir().expect("temporary process environment");
    let loxa_home = temp.path().join("loxa-home");
    let output = controlled_command(temp.path())
        .args(["pull", "unmistakable-owner/unmistakable-repo"])
        .output()
        .expect("run selector-less loxa pull");

    assert_eq!(output.status.code(), Some(2), "{output:?}");
    assert_eq!(output.stdout, b"");
    assert_eq!(
        String::from_utf8(output.stderr).expect("UTF-8 stderr"),
        concat!(
            "error: the following required arguments were not provided:\n",
            "  <--file <FILENAME>|--quant <QUANT>>\n",
            "\n",
            "Usage: loxa pull <--file <FILENAME>|--quant <QUANT>> <REPO>\n",
            "\n",
            "For more information, try '--help'.\n",
        )
    );
    assert!(!loxa_home.exists(), "parser rejection created LOXA_HOME");
}

#[test]
fn empty_list_guidance_requires_explicit_selection() {
    let temp = tempdir().expect("temporary process environment");
    let output = controlled_command(temp.path())
        .arg("list")
        .output()
        .expect("run loxa list");

    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let stdout = String::from_utf8(output.stdout).expect("UTF-8 stdout");
    assert_eq!(
        stdout,
        concat!(
            "No runnable models installed.\n",
            "Choose a GGUF with `loxa inspect <owner/repo>`, then download it with `loxa pull <owner/repo> --file <filename>`.\n",
        )
    );
    assert!(!stdout.contains("`loxa pull <owner/repo>`"), "{stdout}");
    assert_eq!(output.stderr, b"");
}
