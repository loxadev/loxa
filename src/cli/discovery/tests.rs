use super::*;
use crate::cli::{Cli, Command, InspectArgs, SearchArgs};
use crate::discovery::{
    ArtifactCandidate, AuxiliaryRole, CandidateDisposition, DiscoveryError, DiscoveryErrorKind,
    GatedStatus, ModelSearchHit, ModelSearchPage, RepositoryPlan, UnsupportedPackagingReason,
};
use clap::Parser;

#[test]
fn discovery_errors_map_to_exact_static_cli_messages() {
    let cases = [
        (
            DiscoveryErrorKind::InvalidQuery,
            "invalid Hugging Face search query",
        ),
        (
            DiscoveryErrorKind::InvalidRepository,
            "invalid Hugging Face repository; expected owner/repo",
        ),
        (
            DiscoveryErrorKind::InvalidRevision,
            "invalid Hugging Face revision",
        ),
        (
            DiscoveryErrorKind::AuthenticationRequired,
            "Hugging Face authentication is required",
        ),
        (
            DiscoveryErrorKind::AccessDenied,
            "Hugging Face repository access was denied",
        ),
        (
            DiscoveryErrorKind::RepositoryNotFound,
            "Hugging Face repository was not found",
        ),
        (
            DiscoveryErrorKind::RevisionNotFound,
            "Hugging Face revision was not found",
        ),
        (
            DiscoveryErrorKind::RateLimited,
            "Hugging Face rate limit exceeded; try again later",
        ),
        (
            DiscoveryErrorKind::RemoteUnavailable,
            "Hugging Face is unavailable; try again later",
        ),
        (
            DiscoveryErrorKind::DeadlineExceeded,
            "Hugging Face request timed out",
        ),
        (
            DiscoveryErrorKind::RedirectRejected,
            "Hugging Face response was rejected: redirect",
        ),
        (
            DiscoveryErrorKind::PaginationRejected,
            "Hugging Face response was rejected: invalid pagination",
        ),
        (
            DiscoveryErrorKind::ResponseTooLarge,
            "Hugging Face response was rejected: response too large",
        ),
        (
            DiscoveryErrorKind::MalformedResponse,
            "Hugging Face response was rejected: malformed response",
        ),
    ];

    for (kind, expected) in cases {
        assert_eq!(discovery_error_message(kind), expected);
    }
}

#[test]
fn search_execution_forwards_exact_input_once_and_formats_empty_success() {
    let calls = std::cell::Cell::new(0);
    let raw_input = "  hf://Owner/Repo  ";

    let output = execute_search(
        SearchArgs {
            query: raw_input.into(),
        },
        |request| {
            calls.set(calls.get() + 1);
            assert_eq!(request.query(), raw_input);
            Ok(ModelSearchPage::new(Vec::new()))
        },
    )
    .unwrap();

    assert_eq!(calls.get(), 1);
    assert_eq!(output, "Repositories (0)\nNo matching repositories.\n");
}

#[test]
fn search_results_include_one_neutral_inspect_command_per_hit() {
    let output = execute_search(
        SearchArgs {
            query: "gemma".into(),
        },
        |_| {
            Ok(ModelSearchPage::new(vec![
                ModelSearchHit::new("owner/public".into(), GatedStatus::Public, Some(1200)),
                ModelSearchHit::new(
                    "owner/automatic".into(),
                    GatedStatus::AutomaticApproval,
                    Some(7),
                ),
                ModelSearchHit::new("owner/manual".into(), GatedStatus::ManualApproval, Some(0)),
                ModelSearchHit::new("owner/unknown".into(), GatedStatus::Unknown, None),
            ]))
        },
    )
    .unwrap();

    assert_eq!(
        output,
        concat!(
            "Repositories (4)\n",
            "\n",
            "owner/public\n",
            "  Access: Public\n",
            "  Downloads: 1200\n",
            "  Inspect: loxa inspect owner/public\n",
            "\n",
            "owner/automatic\n",
            "  Access: Automatic approval\n",
            "  Downloads: 7\n",
            "  Inspect: loxa inspect owner/automatic\n",
            "\n",
            "owner/manual\n",
            "  Access: Manual approval\n",
            "  Downloads: 0\n",
            "  Inspect: loxa inspect owner/manual\n",
            "\n",
            "owner/unknown\n",
            "  Access: Unknown\n",
            "  Downloads: Unknown\n",
            "  Inspect: loxa inspect owner/unknown\n",
        )
    );
    assert_eq!(output.matches("  Inspect: loxa inspect ").count(), 4);
    let lower = output.to_ascii_lowercase();
    for excluded in ["recommended", "best", "fits", "compatible"] {
        assert!(
            !lower.contains(excluded),
            "unexpected {excluded} in {output}"
        );
    }
}

#[test]
fn search_execution_displays_a_sole_hit_with_only_neutral_inspect_guidance() {
    let output = execute_search(
        SearchArgs {
            query: "owner/sole".into(),
        },
        |_| {
            Ok(ModelSearchPage::new(vec![ModelSearchHit::new(
                "owner/sole".into(),
                GatedStatus::Public,
                None,
            )]))
        },
    )
    .unwrap();

    assert_eq!(
        output,
        concat!(
            "Repositories (1)\n",
            "\n",
            "owner/sole\n",
            "  Access: Public\n",
            "  Downloads: Unknown\n",
            "  Inspect: loxa inspect owner/sole\n",
        )
    );
    for excluded in ["loxa pull", "compatible", "Compatible", "recommend", "best"] {
        assert!(
            !output.contains(excluded),
            "unexpected {excluded} in {output}"
        );
    }
}

#[test]
fn search_execution_errors_never_include_raw_query_or_remote_detail() {
    let raw_query = "raw-query\nhttps://evil.example/body?token=UNIQUE_SEARCH_SECRET";
    let kinds = [
        DiscoveryErrorKind::InvalidQuery,
        DiscoveryErrorKind::InvalidRepository,
        DiscoveryErrorKind::InvalidRevision,
        DiscoveryErrorKind::AuthenticationRequired,
        DiscoveryErrorKind::AccessDenied,
        DiscoveryErrorKind::RepositoryNotFound,
        DiscoveryErrorKind::RevisionNotFound,
        DiscoveryErrorKind::RateLimited,
        DiscoveryErrorKind::RemoteUnavailable,
        DiscoveryErrorKind::DeadlineExceeded,
        DiscoveryErrorKind::RedirectRejected,
        DiscoveryErrorKind::PaginationRejected,
        DiscoveryErrorKind::ResponseTooLarge,
        DiscoveryErrorKind::MalformedResponse,
    ];

    for kind in kinds {
        let error = execute_search(
            SearchArgs {
                query: raw_query.into(),
            },
            |_| Err(DiscoveryError::new(kind)),
        )
        .unwrap_err();

        for secret in ["raw-query", "evil.example", "body", "UNIQUE_SEARCH_SECRET"] {
            assert!(!error.contains(secret), "{kind:?}: {error}");
        }
    }
}

#[test]
fn inspection_execution_forwards_repository_and_optional_revision_once() {
    for revision in [None, Some("refs/pr/7".to_owned())] {
        let calls = std::cell::Cell::new(0);
        let raw_repo = " owner/repo ";
        let output = execute_inspect(
            InspectArgs {
                repo: raw_repo.into(),
                revision: revision.clone(),
            },
            |request| {
                calls.set(calls.get() + 1);
                assert_eq!(request.repo(), raw_repo);
                assert_eq!(request.revision(), revision.as_deref());
                Ok(RepositoryPlan::new(
                    "owner/repo".into(),
                    "0123456789abcdef0123456789abcdef01234567".into(),
                    Vec::new(),
                ))
            },
        )
        .unwrap();

        assert_eq!(calls.get(), 1);
        assert_eq!(
            output,
            concat!(
                "Repository: owner/repo\n",
                "Commit: 0123456789abcdef0123456789abcdef01234567\n",
                "Runtime compatibility: Unknown (local validation not run)\n",
                "GGUF candidates (0; 0 eligible)\n",
            )
        );
    }
}

#[test]
fn inspection_execution_formats_one_eligible_candidate_with_full_identity() {
    let sha256 = "a".repeat(64);
    let identity = crate::huggingface::test_resolved_file(sha256.clone(), 4_512_345_678);
    let output = execute_inspect(
        InspectArgs {
            repo: "owner/repo".into(),
            revision: None,
        },
        |_| {
            Ok(RepositoryPlan::new(
                "owner/repo".into(),
                "0123456789abcdef0123456789abcdef01234567".into(),
                vec![ArtifactCandidate::new(
                    "model-Q4_K_M.gguf".into(),
                    Some(4_512_345_678),
                    Some(identity),
                    CandidateDisposition::EligibleForDownloadAndLocalValidation,
                )],
            ))
        },
    )
    .unwrap();

    assert_eq!(
        output,
        concat!(
            "Repository: owner/repo\n",
            "Commit: 0123456789abcdef0123456789abcdef01234567\n",
            "Runtime compatibility: Unknown (local validation not run)\n",
            "GGUF candidates (1; 1 eligible)\n",
            "\n",
            "model-Q4_K_M.gguf\n",
            "  Size: 4512345678 bytes\n",
            "  Packaging: Eligible for download and local validation\n",
            "  SHA-256: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n",
            "  Pull: loxa pull 'hf.co/owner/repo:model.gguf'\n",
        )
    );
    assert!(output.contains(&sha256));
}

fn eligible_candidate(repo: &str, path: &str, size: u64) -> ArtifactCandidate {
    ArtifactCandidate::new(
        path.into(),
        Some(size),
        Some(crate::huggingface::test_resolved_file_for(
            repo,
            path,
            "a".repeat(64),
            size,
        )),
        CandidateDisposition::EligibleForDownloadAndLocalValidation,
    )
}

#[test]
fn inspection_execution_prints_exact_commands_in_candidate_order_without_claims() {
    let output = execute_inspect(
        InspectArgs {
            repo: "owner/repo".into(),
            revision: None,
        },
        |_| {
            Ok(RepositoryPlan::new(
                "owner/repo".into(),
                "0123456789abcdef0123456789abcdef01234567".into(),
                vec![
                    eligible_candidate("owner/repo", "first-Q4_K_M.gguf", 10),
                    ArtifactCandidate::new(
                        "unsupported.gguf".into(),
                        Some(15),
                        Some(crate::huggingface::test_resolved_file_for(
                            "owner/repo",
                            "unsupported.gguf",
                            "b".repeat(64),
                            15,
                        )),
                        CandidateDisposition::UnsupportedPackaging(
                            UnsupportedPackagingReason::Auxiliary(AuxiliaryRole::Draft),
                        ),
                    ),
                    eligible_candidate("owner/repo", "second-Q4_K_M.gguf", 20),
                ],
            ))
        },
    )
    .unwrap();

    let first = "  Pull: loxa pull 'hf.co/owner/repo:first-Q4_K_M.gguf'";
    let second = "  Pull: loxa pull 'hf.co/owner/repo:second-Q4_K_M.gguf'";
    let first_position = output.find(first).expect("first exact pull command");
    let second_position = output.find(second).expect("second exact pull command");
    assert!(first_position < second_position, "{output}");
    assert_eq!(output.matches("  Pull:").count(), 2, "{output}");
    assert!(!output.contains("hf.co/owner/repo:unsupported.gguf"));
    assert!(
        output.contains("Runtime compatibility: Unknown (local validation not run)"),
        "{output}"
    );
    let lower = output.to_ascii_lowercase();
    for excluded in ["recommended", "best", "fits", "compatible"] {
        assert!(
            !lower.contains(excluded),
            "unexpected {excluded} in {output}"
        );
    }
}

#[cfg(unix)]
fn shell_argv(command: &str, directory: &std::path::Path) -> Vec<String> {
    let script = format!("set -- {command}; printf '%s\\n' \"$@\"");
    let output = std::process::Command::new("/bin/sh")
        .args(["-c", &script])
        .current_dir(directory)
        .output()
        .expect("evaluate fixture-generated command words");
    assert!(output.status.success(), "{output:?}");
    assert_eq!(output.stderr, b"");
    String::from_utf8(output.stdout)
        .expect("UTF-8 shell argv")
        .lines()
        .map(str::to_owned)
        .collect()
}

#[cfg(unix)]
#[test]
fn inspection_execution_shell_commands_round_trip_without_evaluation() {
    let temp = tempfile::tempdir().unwrap();
    let filename = "-model ' $(touch compact-dollar) `touch compact-backtick`:tag.gguf";
    let revision = "-release ' $(touch revision-dollar) `touch revision-backtick`";
    let output = execute_inspect(
        InspectArgs {
            repo: "owner/repo".into(),
            revision: Some(revision.into()),
        },
        |_| {
            Ok(RepositoryPlan::new(
                "owner/repo".into(),
                "0123456789abcdef0123456789abcdef01234567".into(),
                vec![eligible_candidate("owner/repo", filename, 42)],
            ))
        },
    )
    .unwrap();

    let command = output
        .lines()
        .find_map(|line| line.strip_prefix("  Pull: "))
        .expect("copyable pull command");
    let argv = shell_argv(command, temp.path());
    assert_eq!(argv.len(), 4, "{argv:?}");
    assert_eq!(argv[0], "loxa");
    assert_eq!(argv[1], "pull");
    assert_eq!(argv[2], format!("hf.co/owner/repo:{filename}"));
    assert_eq!(argv[3], format!("--revision={revision}"));
    for marker in [
        "compact-dollar",
        "compact-backtick",
        "revision-dollar",
        "revision-backtick",
    ] {
        assert!(!temp.path().join(marker).exists(), "created {marker}");
    }

    let parsed = Cli::try_parse_from(argv).unwrap();
    let Command::Pull(args) = parsed.command else {
        panic!("expected pull command");
    };
    let normalized = crate::cli::parse_pull_input(&args).unwrap();
    assert_eq!(normalized.filename.as_deref(), Some(filename));
    assert_eq!(normalized.revision.as_deref(), Some(revision));
}

#[cfg(unix)]
#[test]
fn inspection_execution_compact_and_legacy_fallback_round_trip_exact_filenames() {
    let fallback = format!("{}.gguf", "x".repeat(251));
    assert_eq!(fallback.len(), 256);
    let output = execute_inspect(
        InspectArgs {
            repo: "owner/repo".into(),
            revision: None,
        },
        |_| {
            Ok(RepositoryPlan::new(
                "owner/repo".into(),
                "0123456789abcdef0123456789abcdef01234567".into(),
                vec![
                    eligible_candidate("owner/repo", "model?#.gguf", 1),
                    eligible_candidate("owner/repo", &fallback, 2),
                ],
            ))
        },
    )
    .unwrap();

    let commands = output
        .lines()
        .filter_map(|line| line.strip_prefix("  Pull: "))
        .collect::<Vec<_>>();
    assert_eq!(commands.len(), 2, "{output}");
    assert!(commands[0].contains("'hf.co/owner/repo:model?#.gguf'"));
    assert!(commands[1].contains("owner/repo --file="));

    for (command, expected_filename) in commands.into_iter().zip(["model?#.gguf", &fallback]) {
        let directory = tempfile::tempdir().unwrap();
        let argv = shell_argv(command, directory.path());
        let parsed = Cli::try_parse_from(argv).unwrap();
        let Command::Pull(args) = parsed.command else {
            panic!("expected pull command");
        };
        let normalized = crate::cli::parse_pull_input(&args).unwrap();
        assert_eq!(normalized.filename.as_deref(), Some(expected_filename));
        assert_eq!(normalized.revision, None);
    }
}

#[test]
fn inspection_execution_omits_pull_commands_without_candidate_identity() {
    let output = execute_inspect(
        InspectArgs {
            repo: "owner/repo".into(),
            revision: Some("main".into()),
        },
        |_| {
            Ok(RepositoryPlan::new(
                "owner/repo".into(),
                "fedcba9876543210fedcba9876543210fedcba98".into(),
                vec![
                    ArtifactCandidate::new(
                        "first.gguf".into(),
                        Some(10),
                        None,
                        CandidateDisposition::EligibleForDownloadAndLocalValidation,
                    ),
                    ArtifactCandidate::new(
                        "unsupported.gguf".into(),
                        None,
                        None,
                        CandidateDisposition::UnsupportedPackaging(
                            UnsupportedPackagingReason::MissingSize,
                        ),
                    ),
                    ArtifactCandidate::new(
                        "second.gguf".into(),
                        Some(20),
                        None,
                        CandidateDisposition::EligibleForDownloadAndLocalValidation,
                    ),
                ],
            ))
        },
    )
    .unwrap();

    assert_eq!(
        output,
        concat!(
            "Repository: owner/repo\n",
            "Commit: fedcba9876543210fedcba9876543210fedcba98\n",
            "Runtime compatibility: Unknown (local validation not run)\n",
            "GGUF candidates (3; 2 eligible)\n",
            "\n",
            "first.gguf\n",
            "  Size: 10 bytes\n",
            "  Packaging: Eligible for download and local validation\n",
            "\n",
            "unsupported.gguf\n",
            "  Size: Unknown\n",
            "  Packaging: Unsupported (missing size)\n",
            "\n",
            "second.gguf\n",
            "  Size: 20 bytes\n",
            "  Packaging: Eligible for download and local validation\n",
        )
    );
    for excluded in [
        "loxa pull",
        "--file",
        "fits",
        "recommended",
        "best",
        "runnable",
        "engine compatible",
    ] {
        assert!(
            !output.contains(excluded),
            "unexpected {excluded} in {output}"
        );
    }
}

#[test]
fn inspection_execution_maps_every_unsupported_packaging_reason_exactly() {
    let cases = [
        (
            "entry.txt",
            Some(1),
            UnsupportedPackagingReason::UnsupportedEntryType,
        ),
        (
            "../unsafe.gguf",
            Some(2),
            UnsupportedPackagingReason::UnsafePath,
        ),
        (
            "nested/model.gguf",
            Some(3),
            UnsupportedPackagingReason::NestedPath,
        ),
        (
            "model-00001-of-00002.gguf",
            Some(4),
            UnsupportedPackagingReason::Sharded,
        ),
        (
            "mtp-model.gguf",
            Some(5),
            UnsupportedPackagingReason::Auxiliary(AuxiliaryRole::Mtp),
        ),
        (
            "draft-model.gguf",
            Some(6),
            UnsupportedPackagingReason::Auxiliary(AuxiliaryRole::Draft),
        ),
        (
            "model-mmproj.gguf",
            Some(7),
            UnsupportedPackagingReason::Auxiliary(AuxiliaryRole::Mmproj),
        ),
        (
            "missing-size.gguf",
            None,
            UnsupportedPackagingReason::MissingSize,
        ),
        (
            "zero-size.gguf",
            Some(0),
            UnsupportedPackagingReason::ZeroSize,
        ),
        (
            "missing-lfs.gguf",
            Some(9),
            UnsupportedPackagingReason::MissingLfsIdentity,
        ),
        (
            "size-mismatch.gguf",
            Some(10),
            UnsupportedPackagingReason::SizeMismatch,
        ),
        (
            "invalid-sha.gguf",
            Some(11),
            UnsupportedPackagingReason::InvalidLfsSha256,
        ),
    ];
    let candidates = cases
        .iter()
        .map(|(path, size, reason)| {
            ArtifactCandidate::new(
                (*path).into(),
                *size,
                None,
                CandidateDisposition::UnsupportedPackaging(*reason),
            )
        })
        .collect();

    let output = execute_inspect(
        InspectArgs {
            repo: "owner/repo".into(),
            revision: None,
        },
        |_| {
            Ok(RepositoryPlan::new(
                "owner/repo".into(),
                "0123456789abcdef0123456789abcdef01234567".into(),
                candidates,
            ))
        },
    )
    .unwrap();

    assert_eq!(
        output,
        concat!(
            "Repository: owner/repo\n",
            "Commit: 0123456789abcdef0123456789abcdef01234567\n",
            "Runtime compatibility: Unknown (local validation not run)\n",
            "GGUF candidates (12; 0 eligible)\n",
            "\n",
            "entry.txt\n",
            "  Size: 1 bytes\n",
            "  Packaging: Unsupported (unsupported entry type)\n",
            "\n",
            "../unsafe.gguf\n",
            "  Size: 2 bytes\n",
            "  Packaging: Unsupported (unsafe path)\n",
            "\n",
            "nested/model.gguf\n",
            "  Size: 3 bytes\n",
            "  Packaging: Unsupported (nested path)\n",
            "\n",
            "model-00001-of-00002.gguf\n",
            "  Size: 4 bytes\n",
            "  Packaging: Unsupported (sharded)\n",
            "\n",
            "mtp-model.gguf\n",
            "  Size: 5 bytes\n",
            "  Packaging: Unsupported (MTP auxiliary)\n",
            "\n",
            "draft-model.gguf\n",
            "  Size: 6 bytes\n",
            "  Packaging: Unsupported (draft auxiliary)\n",
            "\n",
            "model-mmproj.gguf\n",
            "  Size: 7 bytes\n",
            "  Packaging: Unsupported (mmproj auxiliary)\n",
            "\n",
            "missing-size.gguf\n",
            "  Size: Unknown\n",
            "  Packaging: Unsupported (missing size)\n",
            "\n",
            "zero-size.gguf\n",
            "  Size: 0 bytes\n",
            "  Packaging: Unsupported (zero size)\n",
            "\n",
            "missing-lfs.gguf\n",
            "  Size: 9 bytes\n",
            "  Packaging: Unsupported (missing LFS identity)\n",
            "\n",
            "size-mismatch.gguf\n",
            "  Size: 10 bytes\n",
            "  Packaging: Unsupported (size mismatch)\n",
            "\n",
            "invalid-sha.gguf\n",
            "  Size: 11 bytes\n",
            "  Packaging: Unsupported (invalid LFS SHA-256)\n",
        )
    );
}

#[test]
fn inspection_execution_errors_never_include_raw_repository_revision_or_remote_detail() {
    let raw_repo = "raw-repo\nhttps://evil.example/body";
    let raw_revision = "raw-revision?token=UNIQUE_INSPECT_SECRET";
    let kinds = [
        DiscoveryErrorKind::InvalidQuery,
        DiscoveryErrorKind::InvalidRepository,
        DiscoveryErrorKind::InvalidRevision,
        DiscoveryErrorKind::AuthenticationRequired,
        DiscoveryErrorKind::AccessDenied,
        DiscoveryErrorKind::RepositoryNotFound,
        DiscoveryErrorKind::RevisionNotFound,
        DiscoveryErrorKind::RateLimited,
        DiscoveryErrorKind::RemoteUnavailable,
        DiscoveryErrorKind::DeadlineExceeded,
        DiscoveryErrorKind::RedirectRejected,
        DiscoveryErrorKind::PaginationRejected,
        DiscoveryErrorKind::ResponseTooLarge,
        DiscoveryErrorKind::MalformedResponse,
    ];

    for kind in kinds {
        let error = execute_inspect(
            InspectArgs {
                repo: raw_repo.into(),
                revision: Some(raw_revision.into()),
            },
            |_| Err(DiscoveryError::new(kind)),
        )
        .unwrap_err();

        for secret in [
            "raw-repo",
            "raw-revision",
            "evil.example",
            "body",
            "UNIQUE_INSPECT_SECRET",
        ] {
            assert!(!error.contains(secret), "{kind:?}: {error}");
        }
    }
}
