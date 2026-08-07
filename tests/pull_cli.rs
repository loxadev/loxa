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

fn selectorless_stderr(repo: &str) -> String {
    selectorless_stderr_with_revision(repo, "")
}

fn selectorless_stderr_with_revision(repo: &str, revision: &str) -> String {
    format!(
        concat!(
            "error: no GGUF was selected for {repo}\n",
            "\n",
            "Inspect every eligible GGUF:\n",
            "  loxa inspect {repo}{revision}\n",
            "\n",
            "Then choose exactly one:\n",
            "  loxa pull {repo} --file <FILENAME>{revision}\n",
            "  loxa pull {repo} --quant <QUANT>{revision}\n",
            "  loxa pull hf.co/{repo}:<FILENAME-or-QUANT>{revision}\n",
            "\n",
            "Usage: loxa pull <OWNER/REPO> (--file <FILENAME>|--quant <QUANT>) [OPTIONS]\n",
            "       loxa pull <HF.CO/OWNER/REPO:FILE-OR-QUANT> [OPTIONS]\n",
            "\n",
            "For more information, try '--help'.\n",
        ),
        repo = repo,
        revision = revision,
    )
}

fn assert_selectorless_host_failure(args: &[&str], revision: &str) {
    let temp = tempdir().expect("temporary process environment");
    let output = controlled_command(temp.path())
        .args(args)
        .output()
        .expect("run selector-less host-form loxa pull");

    assert_eq!(output.status.code(), Some(2), "{output:?}");
    assert_eq!(output.stdout, b"");
    let stderr = String::from_utf8(output.stderr).expect("UTF-8 stderr");
    assert_eq!(
        stderr,
        selectorless_stderr_with_revision("owner/repo", revision)
    );
    if !revision.is_empty() {
        assert_eq!(
            stderr.matches(revision).count(),
            4,
            "revision was not preserved on inspect and every retry: {stderr}"
        );
    }
    assert!(
        temp.path()
            .read_dir()
            .expect("temporary environment directory")
            .next()
            .is_none(),
        "selector-less host-form pull created local state"
    );
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
        selectorless_stderr("unmistakable-owner/unmistakable-repo")
    );
    assert!(!loxa_home.exists(), "parser rejection created LOXA_HOME");
}

#[test]
fn selectorless_pull_wins_before_missing_or_relative_app_paths() {
    for relative_home in [None, Some("relative-loxa-home")] {
        let temp = tempdir().expect("temporary process environment");
        let mut command = controlled_command(temp.path());
        command
            .env_remove("LOXA_HOME")
            .env_remove("HOME")
            .env_remove("USERPROFILE");
        if let Some(relative_home) = relative_home {
            command.env("LOXA_HOME", relative_home);
        }

        let output = command
            .args(["pull", "owner/repo"])
            .output()
            .expect("run selector-less loxa pull with unusable paths");

        assert_eq!(output.status.code(), Some(2), "{output:?}");
        assert_eq!(output.stdout, b"");
        assert_eq!(
            String::from_utf8(output.stderr).expect("UTF-8 stderr"),
            selectorless_stderr("owner/repo")
        );
        assert!(
            temp.path()
                .read_dir()
                .expect("temporary environment directory")
                .next()
                .is_none(),
            "selector-less pull created local state"
        );
    }
}

#[test]
fn recognized_hosts_without_selector_use_canonical_guidance_without_state() {
    for reference in ["hf.co/owner/repo", "huggingface.co/owner/repo"] {
        assert_selectorless_host_failure(&["pull", reference], "");
    }
}

#[test]
fn recognized_hosts_with_empty_selector_use_canonical_guidance_without_state() {
    for reference in ["hf.co/owner/repo:", "huggingface.co/owner/repo:"] {
        assert_selectorless_host_failure(&["pull", reference], "");
    }
}

#[test]
fn selectorless_host_preserves_quoted_revision_on_inspect_and_every_retry() {
    assert_selectorless_host_failure(
        &[
            "pull",
            "huggingface.co/owner/repo",
            "--revision",
            "release candidate",
        ],
        " --revision='release candidate'",
    );
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
