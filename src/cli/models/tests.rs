use super::*;
use crate::catalog::{
    Artifact, ArtifactProvenance, ArtifactRole, Manifest, RuntimeQualification, TEST_LLAMA_BUILD,
    TEST_MTP_PROFILE,
};
use crate::cli::dispatch::run;
use crate::cli::{Cli, RuntimeArgs};
use crate::paths::AppPaths;
use crate::runnable::resolve_runnable;
use clap::Parser;

#[test]
fn incomplete_list_copy_is_human_readable_shell_safe_and_leaves_foreign_parts_untouched() {
    let entries = vec![
        crate::app::IncompleteTransferSummary::new("alpha".into(), 15_900_000, 88_200_000),
        crate::app::IncompleteTransferSummary::new("owner-s-model".into(), 57_000_000, 270_900_000),
    ];

    assert_eq!(
        format_incomplete_transfers(&entries, 2),
        concat!(
            "Incomplete downloads (2)\n",
            "\n",
            "  alpha  18% · 15.9 MB of 88.2 MB\n",
            "    Discard: loxa discard 'alpha'\n",
            "\n",
            "  owner-s-model  21% · 57.0 MB of 270.9 MB\n",
            "    Discard: loxa discard 'owner-s-model'\n",
            "\n",
            "2 unrecognized partial files were left untouched.\n",
        )
    );
}

fn manifest(id: &str) -> Manifest {
    Manifest {
        version: 1,
        id: id.into(),
        repo: Some("owner/repo".into()),
        revision: Some("0".repeat(40)),
        remote_filename: Some("model-Q4_K_M.gguf".into()),
        origin: None,
        source_filename: None,
        local_filename: "model.gguf".into(),
        sha256: "a".repeat(64),
        size: 1,
        artifacts: None,
        profile: None,
        runtime: None,
    }
}

fn test_bundle(id: &str) -> Manifest {
    Manifest {
        version: 3,
        id: id.into(),
        repo: None,
        revision: None,
        remote_filename: None,
        origin: None,
        source_filename: None,
        local_filename: "model.gguf".into(),
        sha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".into(),
        size: 3,
        artifacts: Some(vec![
            Artifact {
                role: ArtifactRole::Model,
                local_filename: "model.gguf".into(),
                sha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".into(),
                size: 3,
                provenance: ArtifactProvenance::Local {
                    source_filename: "model-source.gguf".into(),
                },
            },
            Artifact {
                role: ArtifactRole::Draft,
                local_filename: "draft.gguf".into(),
                sha256: "7743ce348d9284d677a185f33295b92266cc435a5b5f775029b300066d26693a".into(),
                size: 5,
                provenance: ArtifactProvenance::Local {
                    source_filename: "draft-source.gguf".into(),
                },
            },
        ]),
        profile: Some(TEST_MTP_PROFILE.into()),
        runtime: Some(RuntimeQualification {
            engine: "llama.cpp".into(),
            build: TEST_LLAMA_BUILD.into(),
        }),
    }
}

fn install_bundle(paths: &AppPaths, id: &str) -> Manifest {
    let manifest = test_bundle(id);
    let model_dir = paths.model_dir(id).unwrap();
    std::fs::create_dir_all(&model_dir).unwrap();
    std::fs::write(model_dir.join("model.gguf"), b"abc").unwrap();
    std::fs::write(model_dir.join("draft.gguf"), b"draft").unwrap();
    crate::catalog::publish_manifest(&paths.models, &manifest).unwrap();
    manifest
}

fn install(paths: &AppPaths, id: &str) -> Manifest {
    let manifest = Manifest {
        version: 1,
        id: id.into(),
        repo: Some("owner/repo".into()),
        revision: Some("0".repeat(40)),
        remote_filename: Some("model-Q4_K_M.gguf".into()),
        origin: None,
        source_filename: None,
        local_filename: "model.gguf".into(),
        sha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".into(),
        size: 3,
        artifacts: None,
        profile: None,
        runtime: None,
    };
    let model_dir = paths.model_dir(id).unwrap();
    std::fs::create_dir_all(&model_dir).unwrap();
    std::fs::write(model_dir.join("model.gguf"), b"abc").unwrap();
    crate::catalog::publish_manifest(&paths.models, &manifest).unwrap();
    manifest
}

#[test]
fn completed_bundle_size_is_shown_in_list_and_removal_confirmation() {
    let bundle = test_bundle("gemma4");
    bundle.validate().unwrap();

    assert_eq!(installed_model_size(&bundle).to_string(), "8 B");
    assert_eq!(removal_prompt(&bundle), "Remove gemma4 (8 B)?");
}

#[cfg(unix)]
#[test]
fn qualified_bundle_rejects_an_unqualified_explicit_runtime() {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempfile::tempdir().unwrap();
    let paths = AppPaths::from_values(Some(temp.path()), None).unwrap();
    install_bundle(&paths, "gemma4");
    let server = temp.path().join("llama-server");
    std::fs::write(&server, b"#!/bin/sh\nprintf 'version: wrong-build\\n'\n").unwrap();
    std::fs::set_permissions(&server, std::fs::Permissions::from_mode(0o755)).unwrap();

    let result = resolve_runnable(
        "gemma4".into(),
        RuntimeArgs {
            ctx: None,
            port: None,
            server: Some(server),
        },
        &paths,
    );
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("qualified bundle accepted an unqualified runtime"),
    };

    assert!(error.contains("test-build"), "{error}");
}

#[cfg(unix)]
#[test]
fn qualified_bundle_requires_a_verified_draft_before_launch() {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempfile::tempdir().unwrap();
    let paths = AppPaths::from_values(Some(temp.path()), None).unwrap();
    install_bundle(&paths, "gemma4");
    std::fs::write(
        paths.model_dir("gemma4").unwrap().join("draft.gguf"),
        b"broken",
    )
    .unwrap();
    let server = temp.path().join("llama-server");
    std::fs::write(&server, b"#!/bin/sh\nprintf 'test-build\\n'\n").unwrap();
    std::fs::set_permissions(&server, std::fs::Permissions::from_mode(0o755)).unwrap();

    let result = resolve_runnable(
        "gemma4".into(),
        RuntimeArgs {
            ctx: None,
            port: None,
            server: Some(server),
        },
        &paths,
    );
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("qualified bundle accepted a damaged draft"),
    };

    assert!(error.contains("draft.gguf"), "{error}");
}

#[cfg(unix)]
#[test]
fn verified_model_launch_writes_a_receipt_for_future_admission() {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempfile::tempdir().unwrap();
    let paths = AppPaths::from_values(Some(temp.path()), None).unwrap();
    install(&paths, "demo");
    let server = temp.path().join("llama-server");
    std::fs::write(&server, b"#!/bin/sh\nprintf 'version: test\\n'\n").unwrap();
    std::fs::set_permissions(&server, std::fs::Permissions::from_mode(0o755)).unwrap();

    let runnable = resolve_runnable(
        "demo".into(),
        RuntimeArgs {
            ctx: None,
            port: None,
            server: Some(server),
        },
        &paths,
    )
    .unwrap();

    assert_eq!(runnable.launch().id, "demo");
    assert!(
        paths
            .model_dir("demo")
            .unwrap()
            .join("verification-receipt.json")
            .is_file(),
        "a successful full verification must refresh the receipt"
    );
}

#[cfg(unix)]
#[test]
fn receipt_records_the_manifest_and_filesystem_identities_it_will_recheck() {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempfile::tempdir().unwrap();
    let paths = AppPaths::from_values(Some(temp.path()), None).unwrap();
    let manifest = install(&paths, "demo");
    let server = temp.path().join("llama-server");
    std::fs::write(&server, b"#!/bin/sh\nprintf 'version: test\\n'\n").unwrap();
    std::fs::set_permissions(&server, std::fs::Permissions::from_mode(0o755)).unwrap();

    resolve_runnable(
        "demo".into(),
        RuntimeArgs {
            ctx: None,
            port: None,
            server: Some(server),
        },
        &paths,
    )
    .unwrap();

    let receipt: serde_json::Value = serde_json::from_slice(
        &std::fs::read(
            paths
                .model_dir("demo")
                .unwrap()
                .join("verification-receipt.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(receipt["version"], 1);
    assert_eq!(receipt["manifest"]["primary_sha256"], manifest.sha256);
    assert_eq!(receipt["manifest"]["primary_size"], manifest.size);
    assert_eq!(receipt["primary"]["size"], manifest.size);
    assert!(receipt["directory"]["device"].is_u64());
    assert!(receipt["directory"]["inode"].is_u64());
}

#[cfg(unix)]
#[test]
fn model_removal_deletes_its_generated_verification_receipt() {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempfile::tempdir().unwrap();
    let paths = AppPaths::from_values(Some(temp.path()), None).unwrap();
    let manifest = install(&paths, "demo");
    let server = temp.path().join("llama-server");
    std::fs::write(&server, b"#!/bin/sh\nprintf 'version: test\\n'\n").unwrap();
    std::fs::set_permissions(&server, std::fs::Permissions::from_mode(0o755)).unwrap();
    let model_dir = paths.model_dir("demo").unwrap();

    drop(
        resolve_runnable(
            "demo".into(),
            RuntimeArgs {
                ctx: None,
                port: None,
                server: Some(server),
            },
            &paths,
        )
        .unwrap(),
    );
    assert!(model_dir.join("verification-receipt.json").is_file());

    crate::catalog::remove_model(&paths.models, &manifest).unwrap();

    assert!(!model_dir.join("verification-receipt.json").exists());
    assert!(!model_dir.join("manifest.json").exists());
}

#[test]
fn chat_with_missing_model_creates_no_config_or_history_files() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("loxa-home");
    let paths = AppPaths::from_values(Some(&root), None).unwrap();
    let error = run(Cli::parse_from(["loxa", "chat", "missing"]), paths.clone()).unwrap_err();

    assert!(error.contains("unknown model id missing"), "{error}");
    assert!(!paths.config.exists());
    assert!(!root.exists());
}

#[test]
fn chat_without_models_explains_how_to_pull_one() {
    let error = model_options(None, &[]).unwrap_err();

    assert_eq!(
            error,
            "no models installed; choose a GGUF with `loxa inspect <owner/repo>`, then download it with `loxa pull <owner/repo> --file <filename>`"
        );
}

#[test]
fn chat_without_id_auto_selects_one_model() {
    assert_eq!(
        model_options(None, &[manifest("alpha")]).unwrap(),
        ["alpha"]
    );
}

#[test]
fn chat_without_id_offers_all_installed_models() {
    assert_eq!(
        model_options(None, &[manifest("alpha"), manifest("beta")]).unwrap(),
        ["alpha", "beta"]
    );
}

#[test]
fn target_is_the_only_selectable_candidate_when_mtp_and_partial_files_are_present() {
    let temp = tempfile::tempdir().unwrap();
    let paths = AppPaths::from_values(Some(temp.path()), None).unwrap();
    std::fs::create_dir_all(&paths.models).unwrap();
    for name in ["Gemma 4.gguf", "mtp-gemma-4.gguf", "Qwen.gguf.part"] {
        std::fs::write(paths.models.join(name), b"GGUF\x03\0\0\0payload").unwrap();
    }

    let candidates = local_candidates(&paths, &[]).unwrap();
    let runnable = runnable_candidates(&candidates);

    assert_eq!(
        model_options_with_candidates(None, &[], &runnable).unwrap(),
        ["gemma-4"]
    );
    assert!(model_options_with_candidates(Some("mtp-gemma-4".into()), &[], &runnable).is_err());
}

#[test]
fn chat_requires_an_interactive_input_and_output() {
    assert!(ensure_interactive_chat(true, true).is_ok());
    for (stdin, stdout) in [(false, true), (true, false), (false, false)] {
        let error = ensure_interactive_chat(stdin, stdout).unwrap_err();
        assert!(error.contains("interactive terminal"), "{error}");
        assert!(error.contains("loxa chat <id>"), "{error}");
    }
}

#[test]
fn model_selection_only_requires_a_terminal_when_there_are_choices() {
    assert!(matches!(
        select_model("run", None, &[manifest("alpha")], false, false).unwrap(),
        ModelSelection::Selected(id) if id == "alpha"
    ));

    for (stdin, stderr) in [(false, true), (true, false), (false, false)] {
        let error = select_model(
            "run",
            None,
            &[manifest("alpha"), manifest("beta")],
            stdin,
            stderr,
        )
        .unwrap_err();
        assert!(error.contains("loxa run <id>"), "{error}");
    }
}

#[cfg(unix)]
#[test]
fn resolving_a_selected_local_candidate_adopts_it_before_launch() {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempfile::tempdir().unwrap();
    let paths = AppPaths::from_values(Some(temp.path()), None).unwrap();
    std::fs::create_dir_all(&paths.models).unwrap();
    let source = paths.models.join("Gemma 4.gguf");
    std::fs::write(&source, b"GGUF\x03\0\0\0payload").unwrap();
    let server = temp.path().join("llama-server");
    std::fs::write(&server, b"#!/bin/sh\nprintf 'version: test\\n'\n").unwrap();
    std::fs::set_permissions(&server, std::fs::Permissions::from_mode(0o755)).unwrap();

    let runnable = resolve_runnable(
        "gemma-4".into(),
        RuntimeArgs {
            ctx: None,
            port: None,
            server: Some(server),
        },
        &paths,
    )
    .unwrap();

    assert_eq!(runnable.launch().id, "gemma-4");
    assert!(!source.exists());
    assert!(paths.models.join("gemma-4/manifest.json").is_file());
}

#[cfg(unix)]
#[test]
fn invalid_explicit_server_does_not_adopt_an_auto_selected_local_candidate() {
    let temp = tempfile::tempdir().unwrap();
    let paths = AppPaths::from_values(Some(temp.path()), None).unwrap();
    std::fs::create_dir_all(&paths.models).unwrap();
    let source = paths.models.join("Gemma 4.gguf");
    std::fs::write(&source, b"GGUF\x03\0\0\0payload").unwrap();
    let server = temp.path().join("missing-llama-server");

    let error = run(
        Cli::parse_from(["loxa", "run", "--server", server.to_str().unwrap()]),
        paths.clone(),
    )
    .unwrap_err();

    assert!(error.contains("--server is not executable"), "{error}");
    assert!(source.is_file());
    assert!(!paths.models.join("gemma-4/manifest.json").exists());
}

#[test]
fn rm_without_models_reports_an_empty_catalog_without_a_pull_suggestion() {
    let temp = tempfile::tempdir().unwrap();
    let paths = AppPaths::from_values(Some(temp.path()), None).unwrap();

    let error = run(Cli::parse_from(["loxa", "rm", "--yes"]), paths).unwrap_err();

    assert_eq!(error, "no models installed");
}

#[test]
fn rm_yes_removes_the_selected_managed_model() {
    let temp = tempfile::tempdir().unwrap();
    let paths = AppPaths::from_values(Some(temp.path()), None).unwrap();
    install(&paths, "demo");

    assert_eq!(
        run(
            Cli::parse_from(["loxa", "rm", "demo", "--yes"]),
            paths.clone()
        )
        .unwrap(),
        0
    );
    let model_dir = paths.model_dir("demo").unwrap();
    assert!(model_dir.join(".lock").is_file());
    assert!(!model_dir.join("manifest.json").exists());
    assert!(!model_dir.join("model.gguf").exists());
    assert!(crate::catalog::load_catalog(&paths.models)
        .unwrap()
        .is_empty());
}

#[test]
fn rm_without_yes_is_non_destructive_outside_a_terminal() {
    let temp = tempfile::tempdir().unwrap();
    let paths = AppPaths::from_values(Some(temp.path()), None).unwrap();
    install(&paths, "demo");

    let error = run(Cli::parse_from(["loxa", "rm", "demo"]), paths.clone()).unwrap_err();

    assert!(error.contains("pass --yes"), "{error}");
    assert!(paths.model_dir("demo").unwrap().exists());
}

#[test]
fn noninteractive_rm_yes_still_requires_an_explicit_model_id() {
    let temp = tempfile::tempdir().unwrap();
    let paths = AppPaths::from_values(Some(temp.path()), None).unwrap();
    install(&paths, "demo");

    let error = run(Cli::parse_from(["loxa", "rm", "--yes"]), paths.clone()).unwrap_err();

    assert!(error.contains("loxa rm <id> --yes"), "{error}");
    assert!(paths.model_dir("demo").unwrap().exists());
}
