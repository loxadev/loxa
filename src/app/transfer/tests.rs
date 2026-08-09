use super::*;

#[test]
fn resolve_request_exact_file_and_unique_quant_constructors_encode_one_selector() {
    let exact = ResolveArtifactRequest::exact_file(
        "owner/repo".into(),
        Some("main".into()),
        "exact.gguf".into(),
    );
    assert_eq!(exact.repo, "owner/repo");
    assert_eq!(exact.revision.as_deref(), Some("main"));
    assert!(matches!(
        exact.selector,
        ArtifactSelector::ExactFile(ref filename) if filename == "exact.gguf"
    ));

    let quant = ResolveArtifactRequest::unique_quant("owner/repo".into(), None, "Q4_K_M".into());
    assert_eq!(quant.repo, "owner/repo");
    assert_eq!(quant.revision, None);
    assert!(matches!(
        quant.selector,
        ArtifactSelector::UniqueQuant(ref value) if value == "Q4_K_M"
    ));
}

#[test]
fn transfer_control_default_new_and_clones_are_sticky_per_activation() {
    let fresh = TransferControl::new();
    let clone = fresh.clone();
    let defaulted = TransferControl::default();
    assert!(!fresh.pause_requested());
    assert!(!clone.pause_requested());
    assert!(!defaulted.pause_requested());

    clone.request_pause();
    assert!(fresh.pause_requested());
    assert!(clone.pause_requested());
    clone.request_pause();
    assert!(fresh.pause_requested());
    assert!(!defaulted.pause_requested());
}

#[test]
fn old_control_clone_cannot_affect_a_fresh_activation() {
    let old = TransferControl::new();
    let old_clone = old.clone();
    old_clone.request_pause();

    let fresh = TransferControl::new();
    assert!(old.pause_requested());
    assert!(old_clone.pause_requested());
    assert!(!fresh.pause_requested());
}

#[test]
fn resolve_artifact_delegates_exact_file_and_unique_quant_to_incumbent_policy_once() {
    use std::cell::Cell;

    let calls = Cell::new(0);
    let exact =
        crate::huggingface::test_resolved_file_for("owner/repo", "exact.gguf", "a".repeat(64), 7);
    let exact_result = resolve_artifact_with(
        ResolveArtifactRequest::exact_file(
            "owner/repo".into(),
            Some("main".into()),
            "exact.gguf".into(),
        ),
        |repo, revision, filename, quant| {
            calls.set(calls.get() + 1);
            assert_eq!(repo, "owner/repo");
            assert_eq!(revision, Some("main"));
            assert_eq!(filename, Some("exact.gguf"));
            assert_eq!(quant, None);
            Ok::<_, crate::huggingface::ResolveError>(exact.clone())
        },
    )
    .unwrap();
    assert_eq!(exact_result.path(), "exact.gguf");
    assert_eq!(calls.get(), 1);

    let quant =
        crate::huggingface::test_resolved_file_for("owner/repo", "quant.gguf", "b".repeat(64), 9);
    let quant_result = resolve_artifact_with(
        ResolveArtifactRequest::unique_quant("owner/repo".into(), None, "Q4_K_M".into()),
        |repo, revision, filename, selected_quant| {
            calls.set(calls.get() + 1);
            assert_eq!(repo, "owner/repo");
            assert_eq!(revision, None);
            assert_eq!(filename, None);
            assert_eq!(selected_quant, Some("Q4_K_M"));
            Ok::<_, crate::huggingface::ResolveError>(quant.clone())
        },
    )
    .unwrap();
    assert_eq!(quant_result.path(), "quant.gguf");
    assert_eq!(calls.get(), 2);
}

#[test]
fn resolve_artifact_returns_the_exact_incumbent_resolved_file_value() {
    let incumbent = crate::huggingface::test_resolved_file_for(
        "owner/repo",
        "nested/exact.gguf",
        "c".repeat(64),
        42,
    );
    let returned = resolve_artifact_with(
        ResolveArtifactRequest::exact_file(
            "owner/repo".into(),
            Some("main".into()),
            "nested/exact.gguf".into(),
        ),
        |_, _, _, _| Ok::<_, crate::huggingface::ResolveError>(incumbent.clone()),
    )
    .unwrap();
    assert_eq!(returned, incumbent);
}

#[test]
fn resolution_errors_preserve_discovery_and_selection_presentation_without_debug_sources() {
    use crate::discovery::{DiscoveryError, DiscoveryErrorKind};
    use crate::huggingface::{ResolveError, SelectionError};

    let hostile = "https://evil.invalid/model?token=secret /private/model\u{1b}[31m";
    for (cause, display, debug) in [
        (
            ResolveError::Discovery(DiscoveryError::new(DiscoveryErrorKind::RemoteUnavailable)),
            "Hugging Face discovery request failed",
            "ResolveArtifactError { category: Discovery }",
        ),
        (
            ResolveError::Selection(SelectionError::MissingSelection),
            "GGUF selection requires --file or --quant",
            "ResolveArtifactError { category: Selection }",
        ),
        (
            ResolveError::Selection(SelectionError::NoEligibleFiles),
            "repository has no verified single-file GGUF",
            "ResolveArtifactError { category: Selection }",
        ),
        (
            ResolveError::Selection(SelectionError::FileNotFound(hostile.into())),
            "selected verified file was not found; retry with --file <verified filename>",
            "ResolveArtifactError { category: Selection }",
        ),
        (
            ResolveError::Selection(SelectionError::QuantUnavailable {
                requested: hostile.into(),
                available: vec![hostile.into()],
            }),
            "requested quantization is unavailable; retry with --quant <an available value>",
            "ResolveArtifactError { category: Selection }",
        ),
        (
            ResolveError::Selection(SelectionError::AmbiguousQuant {
                requested: hostile.into(),
                filenames: vec![hostile.into()],
            }),
            "requested quantization matched multiple files; use --file <filename> to choose one",
            "ResolveArtifactError { category: Selection }",
        ),
    ] {
        let error = resolve_artifact_with(
            ResolveArtifactRequest::exact_file("owner/repo".into(), None, "model.gguf".into()),
            |_, _, _, _| Err(cause),
        )
        .unwrap_err();
        assert_eq!(error.to_string(), display);
        assert_eq!(format!("{error:?}"), debug);
        for rendered in [error.to_string(), format!("{error:?}")] {
            for secret in ["secret", "evil.invalid", "/private/model", "\u{1b}"] {
                assert!(!rendered.contains(secret));
            }
        }
    }
}

#[test]
fn resolution_and_transfer_errors_are_send_static_and_terminal_safe() {
    fn assert_send_static<T: Send + 'static>() {}
    assert_send_static::<ResolveArtifactError>();
    assert_send_static::<TransferError>();

    let resolution = resolve_artifact_with(
        ResolveArtifactRequest::exact_file("owner/repo".into(), None, "exact.gguf".into()),
        |_, _, _, _| {
            Err(crate::huggingface::ResolveError::Discovery(
                crate::discovery::DiscoveryError::new(
                    crate::discovery::DiscoveryErrorKind::RemoteUnavailable,
                ),
            ))
        },
    )
    .unwrap_err();
    let transfer = TransferError::terminal(TransferErrorKind::UnsafeLocalState);

    for rendered in [
        resolution.to_string(),
        format!("{resolution:?}"),
        transfer.to_string(),
        format!("{transfer:?}"),
    ] {
        assert!(!rendered.contains("secret"));
        assert!(!rendered.contains("evil.invalid"));
        assert!(!rendered.contains("/private/model"));
    }
}

#[test]
fn capacity_rounding_and_all_six_plan_rows_use_hand_derived_literals() {
    let plans = [
        CapacityPlan::CleanInstalled,
        CapacityPlan::InstalledCleanup,
        CapacityPlan::InstalledRepairRequest,
        CapacityPlan::PendingPublish,
        CapacityPlan::PendingRequest,
        CapacityPlan::FreshRequest,
    ];
    for (fragment, manifest_len, size, expected) in [
        (4096, 1, 8192, [0, 0, 12288, 4096, 12288, 16384]),
        (4096, 1, 8193, [0, 0, 12288, 4096, 16384, 20480]),
        (1024, 1025, 2048, [0, 0, 3072, 2048, 4096, 6144]),
        (1024, 1025, 2049, [0, 0, 3072, 2048, 5120, 7168]),
    ] {
        let actual = plans.map(|plan| {
            required_capacity(plan, size, manifest_len, fragment).expect("bounded literals")
        });
        assert_eq!(actual, expected);
    }
}

#[test]
fn aligned_size_uses_mutually_exclusive_phase_maxima_without_an_extra_fragment() {
    assert_eq!(
        required_capacity(CapacityPlan::PendingRequest, 8192, 1, 4096),
        Ok(12288)
    );
    assert_eq!(
        required_capacity(CapacityPlan::FreshRequest, 8192, 1, 4096),
        Ok(16384)
    );
}

#[test]
fn unaligned_size_uses_exact_success_publication_peak() {
    assert_eq!(
        required_capacity(CapacityPlan::PendingRequest, 8193, 1, 4096),
        Ok(16384)
    );
    assert_eq!(
        required_capacity(CapacityPlan::FreshRequest, 8193, 1, 4096),
        Ok(20480)
    );
}

#[test]
fn capacity_equality_passes_and_one_byte_short_fails() {
    assert!(capacity_is_sufficient(16384, 16384));
    assert!(!capacity_is_sufficient(16384, 16383));
}

#[test]
fn capacity_zero_fragment_and_every_add_multiply_round_conversion_overflow_fail_closed() {
    assert_eq!(
        required_capacity(CapacityPlan::FreshRequest, 1, 1, 0),
        Err(TransferErrorKind::CapacityUnavailable)
    );
    assert_eq!(
        round_capacity(u64::MAX, 2),
        Err(TransferErrorKind::CapacityOverflow)
    );
    assert_eq!(
        required_capacity(CapacityPlan::InstalledRepairRequest, u64::MAX, 1, 1),
        Err(TransferErrorKind::CapacityOverflow)
    );
    assert_eq!(
        required_capacity(CapacityPlan::PendingRequest, u64::MAX - 1, 2, 1),
        Err(TransferErrorKind::CapacityOverflow)
    );
    assert_eq!(
        required_capacity(CapacityPlan::FreshRequest, u64::MAX - 1, 1, 1),
        Err(TransferErrorKind::CapacityOverflow)
    );
    assert_eq!(
        checked_available_bytes(u64::MAX, 2),
        Err(TransferErrorKind::CapacityOverflow)
    );
    assert_eq!(
        checked_u64(u64::MAX as u128 + 1),
        Err(TransferErrorKind::CapacityOverflow)
    );
}

#[test]
fn zero_requirement_skips_the_capacity_probe_entirely() {
    let root = tempfile::tempdir().unwrap();
    let model_dir = root.path().join("model");
    let lock = crate::catalog::ModelLock::acquire_for_transfer(&model_dir).unwrap();
    assert_eq!(
        admit_capacity_with(&lock, CapacityPlan::CleanInstalled, 1, 1, |_| {
            panic!("zero requirement must not probe")
        })
        .unwrap(),
        0
    );
}

#[cfg(unix)]
#[test]
fn positive_requirement_probes_the_retained_open_directory_descriptor() {
    use std::cell::Cell;
    use std::os::fd::AsRawFd;

    let root = tempfile::tempdir().unwrap();
    let model_dir = root.path().join("model");
    let lock = crate::catalog::ModelLock::acquire_for_transfer(&model_dir).unwrap();
    let expected_descriptor = lock.model_directory().as_raw_fd();
    let calls = Cell::new(0);
    assert_eq!(
        admit_capacity_with(&lock, CapacityPlan::FreshRequest, 1, 0, |directory| {
            calls.set(calls.get() + 1);
            assert_eq!(directory.as_raw_fd(), expected_descriptor);
            Ok((2, 1))
        })
        .unwrap(),
        2
    );
    assert_eq!(calls.get(), 1);
    let (available, fragment) = available_capacity(lock.model_directory()).unwrap();
    assert!(available > 0);
    assert!(fragment > 0);
}

#[test]
fn changed_directory_path_identity_fails_before_positive_requirement_mutation() {
    use std::cell::Cell;

    let root = tempfile::tempdir().unwrap();
    let model_dir = root.path().join("model");
    let moved = root.path().join("moved-model");
    let lock = crate::catalog::ModelLock::acquire_for_transfer(&model_dir).unwrap();
    std::fs::write(model_dir.join("pinned-witness"), b"pinned").unwrap();
    std::fs::rename(&model_dir, &moved).unwrap();
    std::fs::create_dir(&model_dir).unwrap();
    std::fs::write(model_dir.join("replacement-witness"), b"replacement").unwrap();
    let probes = Cell::new(0);

    assert_eq!(
        admit_capacity_with(&lock, CapacityPlan::FreshRequest, 1, 0, |_| {
            probes.set(probes.get() + 1);
            Ok((2, 1))
        })
        .unwrap_err()
        .kind(),
        TransferErrorKind::UnsafeLocalState
    );
    assert_eq!(probes.get(), 0);
    assert_eq!(
        std::fs::read(moved.join("pinned-witness")).unwrap(),
        b"pinned"
    );
    assert_eq!(
        std::fs::read(model_dir.join("replacement-witness")).unwrap(),
        b"replacement"
    );
}

#[test]
fn combined_plan_covers_all_six_capacity_rows_and_exact_local_states() {
    use crate::catalog::transfer::CatalogTransferState as Catalog;
    use crate::download::plan::ArtifactTransferState as Artifact;

    for (catalog, artifact, expected) in [
        (
            Catalog::Installed,
            Artifact::ValidFinal,
            CapacityPlan::CleanInstalled,
        ),
        (
            Catalog::InstalledCompletionDebris,
            Artifact::ValidFinal,
            CapacityPlan::InstalledCleanup,
        ),
        (
            Catalog::Installed,
            Artifact::InstalledRepair,
            CapacityPlan::InstalledRepairRequest,
        ),
        (
            Catalog::Installed,
            Artifact::Fresh,
            CapacityPlan::InstalledRepairRequest,
        ),
        (
            Catalog::MatchingPending,
            Artifact::ValidFinal,
            CapacityPlan::PendingPublish,
        ),
        (
            Catalog::MatchingPending,
            Artifact::CompletePart,
            CapacityPlan::PendingPublish,
        ),
        (
            Catalog::MatchingPending,
            Artifact::Fresh,
            CapacityPlan::PendingRequest,
        ),
        (
            Catalog::MatchingPending,
            Artifact::RequestCapable,
            CapacityPlan::PendingRequest,
        ),
        (Catalog::Fresh, Artifact::Fresh, CapacityPlan::FreshRequest),
    ] {
        assert_eq!(combine_plans(catalog, artifact), Ok(expected));
    }
}

#[test]
fn combined_plan_refuses_different_malformed_unsafe_local_bundle_and_unidentified_state() {
    use crate::catalog::transfer::CatalogTransferState as Catalog;
    use crate::download::plan::ArtifactTransferState as Artifact;

    assert_eq!(
        combine_plans(Catalog::ArtifactConflict, Artifact::Fresh),
        Err(TransferErrorKind::ArtifactConflict)
    );
    for (catalog, artifact) in [
        (Catalog::Unsafe, Artifact::Fresh),
        (Catalog::Fresh, Artifact::Unsafe),
        (Catalog::Fresh, Artifact::RequestCapable),
        (Catalog::Installed, Artifact::ValidFinalRepairDebris),
        (
            Catalog::InstalledCompletionDebris,
            Artifact::ValidFinalRepairDebris,
        ),
        (Catalog::MatchingPending, Artifact::ValidFinalRepairDebris),
    ] {
        assert_eq!(
            combine_plans(catalog, artifact),
            Err(TransferErrorKind::UnsafeLocalState)
        );
    }
}

fn test_service(root: &std::path::Path) -> AppService {
    AppService::from_paths(crate::paths::AppPaths::from_values(Some(root), None).unwrap())
}

fn test_artifact(bytes: &[u8]) -> ResolvedFile {
    use sha2::{Digest, Sha256};

    let sha256 = Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    crate::huggingface::test_resolved_file(sha256, bytes.len() as u64)
}

fn seed_published_remote(root: &std::path::Path, manifest: &Manifest, bytes: &[u8]) {
    manifest.validate().unwrap();
    let model_dir = root.join("models").join(&manifest.id);
    let lock = crate::catalog::ModelLock::acquire_for_transfer(&model_dir).unwrap();
    drop(lock);
    std::fs::write(
        model_dir.join("manifest.json"),
        serde_json::to_vec_pretty(manifest).unwrap(),
    )
    .unwrap();
    std::fs::write(model_dir.join("model.gguf"), bytes).unwrap();
}

fn exercise_selected_without_requested_id(
    service: &AppService,
    artifact: ResolvedFile,
) -> (
    String,
    TransferDisposition,
    Vec<TransferPhase>,
    usize,
    usize,
    usize,
) {
    use std::cell::{Cell, RefCell};

    let progress = RefCell::new(Vec::new());
    let capacity_calls = Cell::new(0);
    let token_calls = Cell::new(0);
    let download_calls = Cell::new(0);
    let result = transfer_selected_with(
        service,
        TransferSelected::new(artifact, None),
        TransferControl::new(),
        |update| progress.borrow_mut().push(update.phase()),
        |_| {
            capacity_calls.set(capacity_calls.get() + 1);
            Ok((u64::MAX, 1))
        },
        || {
            token_calls.set(token_calls.get() + 1);
            None
        },
        |_, model_dir, _, _, _| {
            download_calls.set(download_calls.get() + 1);
            let final_path = model_dir.join("model.gguf");
            std::fs::write(&final_path, b"abcdef").unwrap();
            Ok(DownloadTerminalOutcome::Complete(
                crate::download::DownloadOutcome::Pulled(final_path),
            ))
        },
    )
    .unwrap();
    (
        result.model_id().to_owned(),
        result.disposition(),
        progress.into_inner(),
        capacity_calls.get(),
        token_calls.get(),
        download_calls.get(),
    )
}

#[test]
fn alternate_id_reuse_requires_exact_v1_identity_and_chooses_sorted_match() {
    let artifact = test_artifact(b"abcdef");

    let exact_root = tempfile::tempdir().unwrap();
    let exact_installed = exact_manifest("custom-model".into(), &artifact);
    seed_published_remote(exact_root.path(), &exact_installed, b"abcdef");
    let exact =
        exercise_selected_without_requested_id(&test_service(exact_root.path()), artifact.clone());

    let mut mismatch_results = Vec::new();
    for (label, mutate) in [
        (
            "repository",
            (|manifest: &mut Manifest| manifest.repo = Some("other/repo".into()))
                as fn(&mut Manifest),
        ),
        ("commit", |manifest: &mut Manifest| {
            manifest.revision = Some("fedcba9876543210fedcba9876543210fedcba98".into())
        }),
        ("path", |manifest: &mut Manifest| {
            manifest.remote_filename = Some("different.gguf".into())
        }),
        ("sha256", |manifest: &mut Manifest| {
            manifest.sha256 = "e".repeat(64)
        }),
        ("size", |manifest: &mut Manifest| manifest.size = 7),
    ] {
        let root = tempfile::tempdir().unwrap();
        let mut installed = exact_manifest("custom-model".into(), &artifact);
        mutate(&mut installed);
        seed_published_remote(root.path(), &installed, b"abcdef");
        mismatch_results.push((
            label,
            exercise_selected_without_requested_id(&test_service(root.path()), artifact.clone()),
        ));
    }

    let duplicate_root = tempfile::tempdir().unwrap();
    for id in ["z-custom", "a-custom"] {
        seed_published_remote(
            duplicate_root.path(),
            &exact_manifest(id.into(), &artifact),
            b"abcdef",
        );
    }
    let duplicate = exercise_selected_without_requested_id(
        &test_service(duplicate_root.path()),
        artifact.clone(),
    );

    assert_eq!(exact.0, "custom-model");
    assert_eq!(exact.1, TransferDisposition::AlreadyInstalled);
    assert!(exact.2.is_empty());
    assert_eq!((exact.3, exact.4, exact.5), (0, 0, 0));

    let deterministic = deterministic_model_id(&artifact);
    for (label, result) in mismatch_results {
        assert_eq!(result.0, deterministic, "{label}");
        assert_eq!(result.1, TransferDisposition::Installed, "{label}");
        assert_eq!(result.2, [TransferPhase::Publishing], "{label}");
        assert_eq!((result.3, result.4, result.5), (1, 1, 1), "{label}");
    }

    assert_eq!(duplicate.0, "a-custom");
    assert_eq!(duplicate.1, TransferDisposition::AlreadyInstalled);
    assert!(duplicate.2.is_empty());
    assert_eq!((duplicate.3, duplicate.4, duplicate.5), (0, 0, 0));
}

#[test]
fn alternate_lookup_catalog_ambiguity_fails_before_transfer_work() {
    use std::cell::Cell;

    let root = tempfile::tempdir().unwrap();
    let service = test_service(root.path());
    let artifact = test_artifact(b"abcdef");
    let malformed_dir = root.path().join("models/malformed");
    std::fs::create_dir_all(&malformed_dir).unwrap();
    std::fs::write(malformed_dir.join("manifest.json"), b"not json").unwrap();
    let progress_calls = Cell::new(0);
    let capacity_calls = Cell::new(0);
    let token_calls = Cell::new(0);
    let download_calls = Cell::new(0);

    let result = transfer_selected_with(
        &service,
        TransferSelected::new(artifact.clone(), None),
        TransferControl::new(),
        |_| progress_calls.set(progress_calls.get() + 1),
        |_| {
            capacity_calls.set(capacity_calls.get() + 1);
            Ok((u64::MAX, 1))
        },
        || {
            token_calls.set(token_calls.get() + 1);
            None
        },
        |_, _, _, _, _| {
            download_calls.set(download_calls.get() + 1);
            Err(DownloadFailure::Durability)
        },
    );
    let error = match result {
        Ok(_) => panic!("ambiguous catalog must refuse"),
        Err(error) => error,
    };

    assert_eq!(error.kind(), TransferErrorKind::UnsafeLocalState);
    assert_eq!(progress_calls.get(), 0);
    assert_eq!(capacity_calls.get(), 0);
    assert_eq!(token_calls.get(), 0);
    assert_eq!(download_calls.get(), 0);
    assert!(!root
        .path()
        .join("models")
        .join(deterministic_model_id(&artifact))
        .exists());
}

#[test]
fn alternate_reuse_scan_to_lock_change_fails_before_artifact_hash_or_transfer_work() {
    use std::cell::Cell;

    for replacement in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let service = test_service(root.path());
        let artifact = test_artifact(b"abcdef");
        let installed = exact_manifest("custom-model".into(), &artifact);
        seed_published_remote(root.path(), &installed, b"abcdef");
        let model_dir = root.path().join("models/custom-model");
        let control = TransferControl::new();
        control.request_pause();
        let progress_calls = Cell::new(0);
        let capacity_calls = Cell::new(0);
        let token_calls = Cell::new(0);
        let download_calls = Cell::new(0);

        let result = transfer_selected_with_lookup_observer(
            &service,
            (
                TransferSelected::new(artifact.clone(), None),
                |model_id: &str| {
                    assert_eq!(model_id, "custom-model");
                    if replacement {
                        let mut changed = installed.clone();
                        changed.sha256 = "f".repeat(64);
                        std::fs::write(
                            model_dir.join("manifest.json"),
                            serde_json::to_vec_pretty(&changed).unwrap(),
                        )
                        .unwrap();
                    } else {
                        std::fs::remove_file(model_dir.join("manifest.json")).unwrap();
                    }
                },
            ),
            control,
            |_| progress_calls.set(progress_calls.get() + 1),
            |_| {
                capacity_calls.set(capacity_calls.get() + 1);
                Ok((u64::MAX, 1))
            },
            || {
                token_calls.set(token_calls.get() + 1);
                None
            },
            |_, _, _, _, _| {
                download_calls.set(download_calls.get() + 1);
                Err(DownloadFailure::Durability)
            },
        );
        let error = match result {
            Ok(_) => panic!("scan-to-lock manifest change must refuse"),
            Err(error) => error,
        };

        assert_eq!(error.kind(), TransferErrorKind::UnsafeLocalState);
        assert_eq!(progress_calls.get(), 0);
        assert_eq!(capacity_calls.get(), 0);
        assert_eq!(token_calls.get(), 0);
        assert_eq!(download_calls.get(), 0);
        assert!(!model_dir.join("pending.json").exists());
        assert!(!root
            .path()
            .join("models")
            .join(deterministic_model_id(&artifact))
            .exists());
    }
}

#[test]
fn alternate_reuse_refuses_missing_and_corrupt_installed_artifact_repair() {
    use std::cell::Cell;

    for (label, final_bytes) in [("missing", None), ("corrupt", Some(b"ghijkl".as_slice()))] {
        let root = tempfile::tempdir().unwrap();
        let service = test_service(root.path());
        let artifact = test_artifact(b"abcdef");
        let installed = exact_manifest("custom-model".into(), &artifact);
        let model_dir = root.path().join("models/custom-model");
        let lock = crate::catalog::ModelLock::acquire_for_transfer(&model_dir).unwrap();
        drop(lock);
        std::fs::write(
            model_dir.join("manifest.json"),
            serde_json::to_vec_pretty(&installed).unwrap(),
        )
        .unwrap();
        if let Some(bytes) = final_bytes {
            std::fs::write(model_dir.join("model.gguf"), bytes).unwrap();
        }
        let progress_calls = Cell::new(0);
        let capacity_calls = Cell::new(0);
        let token_calls = Cell::new(0);
        let download_calls = Cell::new(0);

        let result = transfer_selected_with(
            &service,
            TransferSelected::new(artifact, None),
            TransferControl::new(),
            |_| progress_calls.set(progress_calls.get() + 1),
            |_| {
                capacity_calls.set(capacity_calls.get() + 1);
                Ok((u64::MAX, 1))
            },
            || {
                token_calls.set(token_calls.get() + 1);
                None
            },
            |_, _, _, _, _| {
                download_calls.set(download_calls.get() + 1);
                Err(DownloadFailure::Durability)
            },
        );
        let error = match result {
            Ok(_) => panic!("alternate {label} repair must refuse"),
            Err(error) => error,
        };

        assert_eq!(error.kind(), TransferErrorKind::UnsafeLocalState, "{label}");
        assert_eq!(progress_calls.get(), 0, "{label}");
        assert_eq!(capacity_calls.get(), 0, "{label}");
        assert_eq!(token_calls.get(), 0, "{label}");
        assert_eq!(download_calls.get(), 0, "{label}");
        assert!(model_dir.join("manifest.json").exists(), "{label}");
        assert!(!model_dir.join("pending.json").exists(), "{label}");
    }
}

#[test]
fn fresh_insufficient_disk_creates_only_stable_directory_and_lock() {
    let root = tempfile::tempdir().unwrap();
    let service = test_service(root.path());
    let artifact = test_artifact(b"abcdef");
    let error = match transfer_selected_with(
        &service,
        TransferSelected::new(artifact, Some("demo".into())),
        TransferControl::new(),
        |_| {},
        |_| Ok((0, 4096)),
        || panic!("capacity rejection must precede token lookup"),
        |_, _, _, _, _| panic!("capacity rejection must precede download"),
    ) {
        Ok(_) => panic!("insufficient capacity must refuse"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), TransferErrorKind::InsufficientDisk);
    assert!(error.required_available_bytes().unwrap() > 0);
    assert_eq!(error.available_bytes(), Some(0));
    assert_eq!(error.recovery_model_id(), None);
    assert_eq!(error.recovery_artifact(), None);
    assert_eq!(error.retained_bytes(), None);
    assert!(!error.discardable());

    let model_dir = root.path().join("models/demo");
    let mut names = std::fs::read_dir(&model_dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, [std::ffi::OsString::from(".lock")]);
}

#[cfg(unix)]
fn exact_directory_snapshot(
    path: &std::path::Path,
) -> std::collections::BTreeMap<String, (Vec<u8>, u64, u64, u64)> {
    use std::os::unix::fs::MetadataExt;

    std::fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let metadata = entry.metadata().unwrap();
            (
                entry.file_name().into_string().unwrap(),
                (
                    std::fs::read(entry.path()).unwrap(),
                    metadata.dev(),
                    metadata.ino(),
                    metadata.nlink(),
                ),
            )
        })
        .collect()
}

#[cfg(unix)]
#[test]
fn resume_insufficient_disk_preserves_pending_staging_temps_bytes_and_identities() {
    let root = tempfile::tempdir().unwrap();
    let service = test_service(root.path());
    let artifact = test_artifact(b"abcdef");
    let model_dir = root.path().join("models/demo");
    std::fs::create_dir_all(&model_dir).unwrap();
    let lock = crate::catalog::ModelLock::acquire_for_transfer(&model_dir).unwrap();
    drop(lock);
    crate::catalog::prepare_pull(&model_dir, &exact_manifest("demo".into(), &artifact)).unwrap();
    std::fs::write(model_dir.join("model.gguf.part"), b"abc").unwrap();
    std::fs::write(model_dir.join("pending.json.tmp"), b"pending temp").unwrap();
    std::fs::write(model_dir.join("manifest.json.tmp"), b"manifest temp").unwrap();
    let before = exact_directory_snapshot(&model_dir);

    let error = match transfer_selected_with(
        &service,
        TransferSelected::new(artifact, Some("demo".into())),
        TransferControl::new(),
        |_| {},
        |_| Ok((0, 4096)),
        || panic!("resume capacity rejection must precede token lookup"),
        |_, _, _, _, _| panic!("resume capacity rejection must precede download"),
    ) {
        Ok(_) => panic!("insufficient resume must refuse"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), TransferErrorKind::InsufficientDisk);
    assert_eq!(exact_directory_snapshot(&model_dir), before);
}

#[test]
fn matching_pending_insufficient_disk_attaches_only_audited_recovery_authority() {
    for (model_id, part, restart, final_bytes, invalid, retained, discardable) in [
        ("fresh", None, None, None, false, 0, true),
        ("part", Some(b"abc".as_slice()), None, None, false, 3, true),
        (
            "restart",
            Some(b"abc".as_slice()),
            Some(b"abcde".as_slice()),
            None,
            false,
            5,
            true,
        ),
        (
            "complete",
            Some(b"abcdef".as_slice()),
            None,
            None,
            false,
            6,
            true,
        ),
        (
            "valid-final",
            None,
            None,
            Some(b"abcdef".as_slice()),
            false,
            6,
            false,
        ),
        (
            "repair",
            Some(b"abc".as_slice()),
            None,
            None,
            true,
            3,
            false,
        ),
    ] {
        let root = tempfile::tempdir().unwrap();
        let service = test_service(root.path());
        let artifact = test_artifact(b"abcdef");
        let model_dir = seed_pending(root.path(), model_id, &artifact);
        if let Some(bytes) = part {
            std::fs::write(model_dir.join("model.gguf.part"), bytes).unwrap();
        }
        if let Some(bytes) = restart {
            std::fs::write(model_dir.join("model.gguf.part.restart"), bytes).unwrap();
        }
        if let Some(bytes) = final_bytes {
            std::fs::write(model_dir.join("model.gguf"), bytes).unwrap();
        }
        if invalid {
            std::fs::write(model_dir.join("model.gguf.invalid"), b"repair evidence").unwrap();
        }

        let error = match transfer_selected_with(
            &service,
            TransferSelected::new(artifact.clone(), Some(model_id.into())),
            TransferControl::new(),
            |_| {},
            |_| Ok((0, 4096)),
            || panic!("capacity refusal must precede token lookup"),
            |_, _, _, _, _| panic!("capacity refusal must precede download"),
        ) {
            Ok(_) => panic!("insufficient capacity must refuse"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), TransferErrorKind::InsufficientDisk);
        assert!(error.required_available_bytes().unwrap() > 0);
        assert_eq!(error.available_bytes(), Some(0));
        assert_eq!(error.recovery_model_id(), Some(model_id));
        assert_eq!(error.recovery_artifact(), Some(&artifact));
        assert_eq!(error.retained_bytes(), Some(retained));
        assert_eq!(error.discardable(), discardable);
    }
}

#[test]
fn capacity_unavailable_and_overflow_make_zero_token_or_artifact_requests() {
    use std::cell::Cell;

    for (model_id, artifact, probe, expected) in [
        (
            "unavailable",
            test_artifact(b"abcdef"),
            Err(TransferErrorKind::CapacityUnavailable),
            TransferErrorKind::CapacityUnavailable,
        ),
        (
            "overflow",
            crate::huggingface::test_resolved_file("a".repeat(64), u64::MAX),
            Ok((u64::MAX, 2)),
            TransferErrorKind::CapacityOverflow,
        ),
    ] {
        let root = tempfile::tempdir().unwrap();
        let service = test_service(root.path());
        let token_calls = Cell::new(0);
        let download_calls = Cell::new(0);
        let result = transfer_selected_with(
            &service,
            TransferSelected::new(artifact, Some(model_id.into())),
            TransferControl::new(),
            |_| {},
            |_| probe,
            || {
                token_calls.set(token_calls.get() + 1);
                None
            },
            |_, _, _, _, _| {
                download_calls.set(download_calls.get() + 1);
                Err(DownloadFailure::Durability)
            },
        );
        let error = match result {
            Ok(_) => panic!("capacity failure must refuse"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), expected);
        assert_eq!(token_calls.get(), 0);
        assert_eq!(download_calls.get(), 0);

        let model_dir = root.path().join("models").join(model_id);
        let mut names = std::fs::read_dir(model_dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        names.sort();
        assert_eq!(names, [std::ffi::OsString::from(".lock")]);
    }
}

#[test]
fn generated_catalog_manifest_over_limit_fails_before_transfer_mutation() {
    let root = tempfile::tempdir().unwrap();
    let service = test_service(root.path());
    let remote_filename = format!(
        "{}.gguf",
        "a".repeat(crate::catalog::transfer::MAX_CATALOG_MANIFEST_BYTES)
    );
    let artifact = crate::huggingface::test_resolved_file_for(
        "owner/repo",
        &remote_filename,
        "a".repeat(64),
        1,
    );
    let result = transfer_selected_with(
        &service,
        TransferSelected::new(artifact, Some("demo".into())),
        TransferControl::new(),
        |_| {},
        |_| panic!("oversized manifest must fail before capacity"),
        || panic!("oversized manifest must fail before token lookup"),
        |_, _, _, _, _| panic!("oversized manifest must fail before download"),
    );
    let error = match result {
        Ok(_) => panic!("oversized manifest must refuse"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), TransferErrorKind::CatalogManifestTooLarge);
    assert!(!root.path().join("models/demo").exists());
}

#[test]
fn admission_happens_before_temp_cleanup_token_lookup_pending_write_or_transport() {
    use std::cell::RefCell;

    let root = tempfile::tempdir().unwrap();
    let service = test_service(root.path());
    let artifact = test_artifact(b"abcdef");
    let model_dir = root.path().join("models/demo");
    let lock = crate::catalog::ModelLock::acquire_for_transfer(&model_dir).unwrap();
    drop(lock);
    std::fs::write(model_dir.join("pending.json.tmp"), b"pending temp").unwrap();
    std::fs::write(model_dir.join("manifest.json.tmp"), b"manifest temp").unwrap();
    let pending_temp = std::fs::read(model_dir.join("pending.json.tmp")).unwrap();
    let manifest_temp = std::fs::read(model_dir.join("manifest.json.tmp")).unwrap();
    let events = RefCell::new(Vec::new());

    let result = transfer_selected_with(
        &service,
        TransferSelected::new(artifact, Some("demo".into())),
        TransferControl::new(),
        |_| {},
        |_| {
            events.borrow_mut().push("capacity");
            Ok((0, 4096))
        },
        || {
            events.borrow_mut().push("token");
            None
        },
        |_, _, _, _, _| {
            events.borrow_mut().push("download");
            Err(DownloadFailure::Durability)
        },
    );
    let error = match result {
        Ok(_) => panic!("insufficient capacity must refuse"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), TransferErrorKind::InsufficientDisk);
    assert_eq!(*events.borrow(), ["capacity"]);
    assert_eq!(
        std::fs::read(model_dir.join("pending.json.tmp")).unwrap(),
        pending_temp
    );
    assert_eq!(
        std::fs::read(model_dir.join("manifest.json.tmp")).unwrap(),
        manifest_temp
    );
    assert!(!model_dir.join("pending.json").exists());
}

#[test]
fn pre_mutation_pause_returns_interrupted_without_pending_state() {
    use std::cell::Cell;

    let root = tempfile::tempdir().unwrap();
    let service = test_service(root.path());
    let artifact = test_artifact(b"abcdef");
    let control = TransferControl::new();
    control.request_pause();
    let capacity_calls = Cell::new(0);
    let result = transfer_selected_with(
        &service,
        TransferSelected::new(artifact, Some("fresh".into())),
        control,
        |_| {},
        |_| {
            capacity_calls.set(capacity_calls.get() + 1);
            Ok((u64::MAX, 1))
        },
        || panic!("pre-mutation pause must precede token lookup"),
        |_, _, _, _, _| panic!("pre-mutation pause must precede download"),
    )
    .unwrap();
    assert_eq!(result.disposition(), TransferDisposition::Interrupted);
    assert_eq!(result.retained_bytes(), None);
    assert_eq!(capacity_calls.get(), 1);
    let fresh_dir = root.path().join("models/fresh");
    let mut names = std::fs::read_dir(fresh_dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, [std::ffi::OsString::from(".lock")]);

    let artifact = test_artifact(&vec![b'a'; 131_073]);
    let model_dir = root.path().join("models/hashing");
    let lock = crate::catalog::ModelLock::acquire_for_transfer(&model_dir).unwrap();
    drop(lock);
    crate::catalog::prepare_pull(&model_dir, &exact_manifest("hashing".into(), &artifact)).unwrap();
    std::fs::write(model_dir.join("model.gguf.part"), vec![b'a'; 131_073]).unwrap();
    #[cfg(unix)]
    let before = exact_directory_snapshot(&model_dir);
    let control = TransferControl::new();
    control.request_pause();
    let result = transfer_selected_with(
        &service,
        TransferSelected::new(artifact, Some("hashing".into())),
        control,
        |_| {},
        |_| panic!("hash-planning interruption must precede capacity"),
        || panic!("hash-planning interruption must precede token lookup"),
        |_, _, _, _, _| panic!("hash-planning interruption must precede download"),
    )
    .unwrap();
    assert_eq!(result.disposition(), TransferDisposition::Interrupted);
    assert_eq!(result.retained_bytes(), None);
    #[cfg(unix)]
    assert_eq!(exact_directory_snapshot(&model_dir), before);
}

#[test]
fn exact_resolved_file_reaches_manifest_and_downloader_unchanged() {
    use std::cell::Cell;

    let root = tempfile::tempdir().unwrap();
    let service = test_service(root.path());
    let artifact = crate::huggingface::test_resolved_file_for(
        "owner/repo",
        "exact-model.gguf",
        "a".repeat(64),
        7,
    );
    let expected = artifact.clone();
    let token_calls = Cell::new(0);
    let download_calls = Cell::new(0);
    let result = transfer_selected_with(
        &service,
        TransferSelected::new(artifact, Some("demo".into())),
        TransferControl::new(),
        |_| {},
        |_| Ok((u64::MAX, 1)),
        || {
            token_calls.set(token_calls.get() + 1);
            Some("test-token".into())
        },
        |received, model_dir, token, control, _| {
            download_calls.set(download_calls.get() + 1);
            assert_eq!(received, &expected);
            assert_eq!(model_dir, root.path().join("models/demo"));
            assert_eq!(token.as_deref(), Some("test-token"));
            assert!(!control.pause_requested());
            let pending: crate::catalog::Manifest =
                serde_json::from_slice(&std::fs::read(model_dir.join("pending.json")).unwrap())
                    .unwrap();
            assert_eq!(pending, exact_manifest("demo".into(), &expected));
            Err(DownloadFailure::Durability)
        },
    );
    assert!(result.is_err());
    assert_eq!(token_calls.get(), 1);
    assert_eq!(download_calls.get(), 1);
}

fn seed_pending(
    root: &std::path::Path,
    model_id: &str,
    artifact: &ResolvedFile,
) -> std::path::PathBuf {
    let model_dir = root.join("models").join(model_id);
    let lock = crate::catalog::ModelLock::acquire_for_transfer(&model_dir).unwrap();
    drop(lock);
    crate::catalog::prepare_pull(&model_dir, &exact_manifest(model_id.into(), artifact)).unwrap();
    model_dir
}

#[test]
fn matching_pending_pause_returns_exact_durable_recovery_facts() {
    let root = tempfile::tempdir().unwrap();
    let service = test_service(root.path());
    let artifact = test_artifact(b"abcdef");
    let model_dir = seed_pending(root.path(), "demo", &artifact);
    std::fs::write(model_dir.join("model.gguf.part"), b"abc").unwrap();

    let result = transfer_selected_with(
        &service,
        TransferSelected::new(artifact.clone(), Some("demo".into())),
        TransferControl::new(),
        |_| {},
        |_| Ok((u64::MAX, 1)),
        || None,
        |_, _, _, _, _| Ok(DownloadTerminalOutcome::Paused { retained_bytes: 3 }),
    )
    .unwrap();
    assert_eq!(result.model_id(), "demo");
    assert_eq!(result.artifact(), &artifact);
    assert_eq!(result.disposition(), TransferDisposition::Paused);
    assert_eq!(result.retained_bytes(), Some(3));
    assert!(result.discardable());
}

#[test]
fn complete_part_pause_is_discardable_but_repair_pause_is_not() {
    for (model_id, part, invalid, retained, discardable) in [
        ("complete", b"abcdef".as_slice(), false, 6, true),
        ("repair", b"abc".as_slice(), true, 3, false),
    ] {
        let root = tempfile::tempdir().unwrap();
        let service = test_service(root.path());
        let artifact = test_artifact(b"abcdef");
        let model_dir = seed_pending(root.path(), model_id, &artifact);
        std::fs::write(model_dir.join("model.gguf.part"), part).unwrap();
        if invalid {
            std::fs::write(model_dir.join("model.gguf.invalid"), b"repair evidence").unwrap();
        }

        let result = transfer_selected_with(
            &service,
            TransferSelected::new(artifact, Some(model_id.into())),
            TransferControl::new(),
            |_| {},
            |_| Ok((u64::MAX, 1)),
            || None,
            |_, _, _, _, _| {
                Ok(DownloadTerminalOutcome::Paused {
                    retained_bytes: retained,
                })
            },
        )
        .unwrap();
        assert_eq!(result.disposition(), TransferDisposition::Paused);
        assert_eq!(result.retained_bytes(), Some(retained));
        assert_eq!(result.discardable(), discardable);
    }
}

#[test]
fn repair_pause_omits_discard_when_installed_authority_exists() {
    let root = tempfile::tempdir().unwrap();
    let service = test_service(root.path());
    let artifact = test_artifact(b"abcdef");
    let model_dir = root.path().join("models/demo");
    let lock = crate::catalog::ModelLock::acquire_for_transfer(&model_dir).unwrap();
    drop(lock);
    std::fs::write(
        model_dir.join("manifest.json"),
        serde_json::to_vec_pretty(&exact_manifest("demo".into(), &artifact)).unwrap(),
    )
    .unwrap();
    std::fs::write(model_dir.join("model.gguf"), b"broken").unwrap();

    let result = transfer_selected_with(
        &service,
        TransferSelected::new(artifact, Some("demo".into())),
        TransferControl::new(),
        |_| {},
        |_| Ok((u64::MAX, 1)),
        || None,
        |_, _, _, _, _| Ok(DownloadTerminalOutcome::Paused { retained_bytes: 3 }),
    )
    .unwrap();
    assert_eq!(result.disposition(), TransferDisposition::Paused);
    assert_eq!(result.retained_bytes(), Some(3));
    assert!(!result.discardable());
}

fn mapped_pending_failure_with_part(
    failure: DownloadFailure,
    part: Option<&[u8]>,
) -> (TransferError, ResolvedFile) {
    let root = tempfile::tempdir().unwrap();
    let service = test_service(root.path());
    let artifact = test_artifact(b"abcdef");
    let model_dir = seed_pending(root.path(), "demo", &artifact);
    if let Some(part) = part {
        std::fs::write(model_dir.join("model.gguf.part"), part).unwrap();
    }
    let result = transfer_selected_with(
        &service,
        TransferSelected::new(artifact.clone(), Some("demo".into())),
        TransferControl::new(),
        |_| {},
        |_| Ok((u64::MAX, 1)),
        || None,
        |_, _, _, _, _| Err(failure),
    );
    let error = match result {
        Ok(_) => panic!("terminal downloader failure must remain an error"),
        Err(error) => error,
    };
    (error, artifact)
}

fn mapped_pending_failure(failure: DownloadFailure) -> (TransferError, ResolvedFile) {
    mapped_pending_failure_with_part(failure, Some(b"abc"))
}

#[test]
fn authoritative_part_integrity_outcome_maps_to_zero_byte_pending_recovery() {
    let (error, artifact) = mapped_pending_failure(DownloadFailure::Integrity {
        retained_bytes: 0,
        authority: download::IntegrityAuthority::PendingOnly,
    });
    assert_eq!(error.kind(), TransferErrorKind::Integrity);
    assert_eq!(error.recovery_model_id(), Some("demo"));
    assert_eq!(error.recovery_artifact(), Some(&artifact));
    assert_eq!(error.retained_bytes(), Some(0));
    assert!(error.discardable());
}

#[test]
fn restart_integrity_outcome_maps_to_old_part_recovery() {
    let (error, artifact) = mapped_pending_failure(DownloadFailure::Integrity {
        retained_bytes: 3,
        authority: download::IntegrityAuthority::PendingOnly,
    });
    assert_eq!(error.kind(), TransferErrorKind::Integrity);
    assert_eq!(error.recovery_model_id(), Some("demo"));
    assert_eq!(error.recovery_artifact(), Some(&artifact));
    assert_eq!(error.retained_bytes(), Some(3));
    assert!(error.discardable());
}

#[test]
fn integrity_cleanup_durability_outcome_maps_to_no_public_recovery_facts() {
    let (error, _) = mapped_pending_failure(DownloadFailure::Durability);
    assert_eq!(error.kind(), TransferErrorKind::Durability);
    assert_eq!(error.recovery_model_id(), None);
    assert_eq!(error.recovery_artifact(), None);
    assert_eq!(error.retained_bytes(), None);
    assert!(!error.discardable());
}

#[test]
fn installed_or_invalid_integrity_authority_maps_to_non_discardable_public_recovery() {
    for installed in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let service = test_service(root.path());
        let artifact = test_artifact(b"abcdef");
        let model_dir = if installed {
            let model_dir = root.path().join("models/demo");
            let lock = crate::catalog::ModelLock::acquire_for_transfer(&model_dir).unwrap();
            drop(lock);
            std::fs::write(
                model_dir.join("manifest.json"),
                serde_json::to_vec_pretty(&exact_manifest("demo".into(), &artifact)).unwrap(),
            )
            .unwrap();
            std::fs::write(model_dir.join("model.gguf"), b"broken").unwrap();
            model_dir
        } else {
            let model_dir = seed_pending(root.path(), "demo", &artifact);
            std::fs::write(model_dir.join("model.gguf.invalid"), b"evidence").unwrap();
            model_dir
        };
        assert!(model_dir.exists());
        let result = transfer_selected_with(
            &service,
            TransferSelected::new(artifact.clone(), Some("demo".into())),
            TransferControl::new(),
            |_| {},
            |_| Ok((u64::MAX, 1)),
            || None,
            |_, _, _, _, _| {
                Err(DownloadFailure::Integrity {
                    retained_bytes: 2,
                    authority: download::IntegrityAuthority::Repair,
                })
            },
        );
        let error = match result {
            Ok(_) => panic!("repair integrity must remain an error"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), TransferErrorKind::Integrity);
        assert_eq!(error.recovery_model_id(), Some("demo"));
        assert_eq!(error.recovery_artifact(), Some(&artifact));
        assert_eq!(error.retained_bytes(), Some(2));
        assert!(!error.discardable());
    }
}

#[test]
fn resumable_remote_and_disk_errors_attach_only_proven_exact_recovery() {
    for (failure, kind, retained_bytes, part) in [
        (
            DownloadFailure::Remote { retained_bytes: 0 },
            TransferErrorKind::Remote,
            0,
            None,
        ),
        (
            DownloadFailure::Remote { retained_bytes: 3 },
            TransferErrorKind::Remote,
            3,
            Some(b"abc".as_slice()),
        ),
        (
            DownloadFailure::DiskExhausted { retained_bytes: 3 },
            TransferErrorKind::DiskExhausted,
            3,
            Some(b"abc".as_slice()),
        ),
    ] {
        let (error, artifact) = mapped_pending_failure_with_part(failure, part);
        assert_eq!(error.kind(), kind);
        assert_eq!(error.recovery_model_id(), Some("demo"));
        assert_eq!(error.recovery_artifact(), Some(&artifact));
        assert_eq!(error.retained_bytes(), Some(retained_bytes));
        assert!(error.discardable());
        assert_eq!(error.required_available_bytes(), None);
        assert_eq!(error.available_bytes(), None);
    }
}

#[test]
fn durability_failure_never_claims_paused_retained_or_discardable() {
    let (error, _) = mapped_pending_failure(DownloadFailure::Durability);
    assert_eq!(error.kind(), TransferErrorKind::Durability);
    assert_eq!(error.retained_bytes(), None);
    assert_eq!(error.recovery_model_id(), None);
    assert_eq!(error.recovery_artifact(), None);
    assert!(!error.discardable());
    assert!(!format!("{error:?}").contains("Paused"));
}

#[test]
fn control_immediately_before_fence_pauses_but_late_control_completion_wins() {
    let root = tempfile::tempdir().unwrap();
    let service = test_service(root.path());
    let artifact = test_artifact(b"abcdef");
    let control = TransferControl::new();
    let paused = transfer_selected_with(
        &service,
        TransferSelected::new(artifact.clone(), Some("paused".into())),
        control.clone(),
        |_| {},
        |_| Ok((u64::MAX, 1)),
        || None,
        |_, _, _, control, _| {
            control.request_pause();
            Ok(DownloadTerminalOutcome::Paused { retained_bytes: 0 })
        },
    )
    .unwrap();
    assert_eq!(paused.disposition(), TransferDisposition::Paused);
    assert_eq!(paused.retained_bytes(), Some(0));
    assert!(!root.path().join("models/paused/manifest.json").exists());

    let control = TransferControl::new();
    let late = transfer_selected_with(
        &service,
        TransferSelected::new(artifact, Some("late".into())),
        control.clone(),
        |_| {},
        |_| Ok((u64::MAX, 1)),
        || None,
        |_, model_dir, _, control, _| {
            let final_path = model_dir.join("model.gguf");
            std::fs::write(&final_path, b"abcdef").unwrap();
            control.request_pause();
            Ok(DownloadTerminalOutcome::Complete(
                crate::download::DownloadOutcome::Pulled(final_path),
            ))
        },
    )
    .unwrap();
    assert_eq!(late.disposition(), TransferDisposition::Installed);
    assert!(control.pause_requested());
    assert!(root.path().join("models/late/manifest.json").exists());
    assert!(!root.path().join("models/late/pending.json").exists());
}

#[test]
fn valid_final_publish_only_checks_control_before_publication_fence() {
    let root = tempfile::tempdir().unwrap();
    let service = test_service(root.path());
    let artifact = test_artifact(b"abcdef");

    let before_dir = seed_pending(root.path(), "before", &artifact);
    std::fs::write(before_dir.join("model.gguf"), b"abcdef").unwrap();
    let control = TransferControl::new();
    let request_pause = control.clone();
    let before = transfer_selected_with(
        &service,
        TransferSelected::new(artifact.clone(), Some("before".into())),
        control,
        |_| {},
        |_| {
            request_pause.request_pause();
            Ok((u64::MAX, 1))
        },
        || panic!("valid-final publication must not look up a token"),
        |_, _, _, _, _| panic!("valid-final publication must not call downloader"),
    )
    .unwrap();
    assert_eq!(before.disposition(), TransferDisposition::Interrupted);
    assert!(before_dir.join("pending.json").exists());
    assert!(!before_dir.join("manifest.json").exists());

    let late_dir = seed_pending(root.path(), "late-final", &artifact);
    std::fs::write(late_dir.join("model.gguf"), b"abcdef").unwrap();
    let control = TransferControl::new();
    let late_pause = control.clone();
    let late = transfer_selected_with(
        &service,
        TransferSelected::new(artifact, Some("late-final".into())),
        control,
        move |progress| {
            if progress.phase() == TransferPhase::Publishing {
                late_pause.request_pause();
            }
        },
        |_| Ok((u64::MAX, 1)),
        || panic!("valid-final publication must not look up a token"),
        |_, _, _, _, _| panic!("valid-final publication must not call downloader"),
    )
    .unwrap();
    assert_eq!(late.disposition(), TransferDisposition::Installed);
    assert!(late_dir.join("manifest.json").exists());
    assert!(!late_dir.join("pending.json").exists());
}

#[test]
fn publication_failure_is_typed_publication_not_paused() {
    let root = tempfile::tempdir().unwrap();
    let service = test_service(root.path());
    let artifact = test_artifact(b"abcdef");
    let control = TransferControl::new();
    let late_pause = control.clone();
    let result = transfer_selected_with(
        &service,
        TransferSelected::new(artifact.clone(), Some("demo".into())),
        control,
        move |progress| {
            if progress.phase() == TransferPhase::Publishing {
                late_pause.request_pause();
            }
        },
        |_| Ok((u64::MAX, 1)),
        || None,
        |_, model_dir, _, _, _| {
            let final_path = model_dir.join("model.gguf");
            std::fs::write(&final_path, b"abcdef").unwrap();
            std::fs::write(model_dir.join("model.gguf.invalid"), b"repair evidence").unwrap();
            std::fs::write(model_dir.join("manifest.json"), b"not json").unwrap();
            Ok(DownloadTerminalOutcome::Complete(
                crate::download::DownloadOutcome::Pulled(final_path),
            ))
        },
    );
    let error = match result {
        Ok(_) => panic!("publication failure must not report completion"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), TransferErrorKind::Publication);
    assert_eq!(error.recovery_model_id(), Some("demo"));
    assert_eq!(error.recovery_artifact(), Some(&artifact));
    assert_eq!(error.retained_bytes(), Some(6));
    assert!(!error.discardable());
    assert!(root.path().join("models/demo/pending.json").exists());
}

#[test]
fn publication_failure_without_a_reproven_exact_final_omits_recovery_facts() {
    let root = tempfile::tempdir().unwrap();
    let service = test_service(root.path());
    let artifact = test_artifact(b"abcdef");
    let result = match transfer_selected_with(
        &service,
        TransferSelected::new(artifact, Some("demo".into())),
        TransferControl::new(),
        |_| {},
        |_| Ok((u64::MAX, 1)),
        || None,
        |_, model_dir, _, _, _| {
            let final_path = model_dir.join("model.gguf");
            std::fs::write(&final_path, b"broken").unwrap();
            Ok(DownloadTerminalOutcome::Complete(
                crate::download::DownloadOutcome::Pulled(final_path),
            ))
        },
    ) {
        Ok(_) => panic!("publication without an exact final must refuse"),
        Err(error) => error,
    };
    assert_eq!(result.kind(), TransferErrorKind::Publication);
    assert_eq!(result.recovery_model_id(), None);
    assert_eq!(result.recovery_artifact(), None);
    assert_eq!(result.retained_bytes(), None);
    assert!(!result.discardable());
    assert!(root.path().join("models/demo/pending.json").exists());
}

#[test]
fn clean_installed_is_read_only_already_installed_without_capacity_probe() {
    use std::cell::Cell;

    let root = tempfile::tempdir().unwrap();
    let service = test_service(root.path());
    let artifact = test_artifact(b"abcdef");
    let model_dir = root.path().join("models/demo");
    let lock = crate::catalog::ModelLock::acquire_for_transfer(&model_dir).unwrap();
    drop(lock);
    std::fs::write(
        model_dir.join("manifest.json"),
        serde_json::to_vec_pretty(&exact_manifest("demo".into(), &artifact)).unwrap(),
    )
    .unwrap();
    std::fs::write(model_dir.join("model.gguf"), b"abcdef").unwrap();
    #[cfg(unix)]
    let before = exact_directory_snapshot(&model_dir);
    let progress_calls = Cell::new(0);

    let result = transfer_selected_with(
        &service,
        TransferSelected::for_installed(artifact, "demo".into()),
        TransferControl::new(),
        |_| progress_calls.set(progress_calls.get() + 1),
        |_| panic!("zero-requirement clean install must not probe capacity"),
        || panic!("clean install must not look up token"),
        |_, _, _, _, _| panic!("clean install must not call downloader"),
    )
    .unwrap();
    assert_eq!(result.disposition(), TransferDisposition::AlreadyInstalled);
    assert_eq!(result.retained_bytes(), None);
    assert_eq!(progress_calls.get(), 0);
    #[cfg(unix)]
    assert_eq!(exact_directory_snapshot(&model_dir), before);
}

#[test]
fn inspected_installed_intent_refuses_stale_directory_or_manifest_before_transfer_work() {
    use std::cell::Cell;

    for label in ["directory-removed", "manifest-removed", "manifest-replaced"] {
        let root = tempfile::tempdir().unwrap();
        let service = test_service(root.path());
        let artifact = test_artifact(b"abcdef");
        let model_dir = root.path().join("models/custom-model");
        let lock = crate::catalog::ModelLock::acquire_for_transfer(&model_dir).unwrap();
        drop(lock);
        std::fs::write(
            model_dir.join("manifest.json"),
            serde_json::to_vec_pretty(&exact_manifest("custom-model".into(), &artifact)).unwrap(),
        )
        .unwrap();
        if label != "manifest-removed" {
            std::fs::write(model_dir.join("model.gguf"), b"abcdef").unwrap();
        }

        let inspected = service.installed_models().unwrap();
        assert_eq!(inspected.len(), 1);
        assert_eq!(inspected[0].id(), "custom-model");
        assert!(inspected[0].matches_remote(&artifact));

        match label {
            "directory-removed" => std::fs::remove_dir_all(&model_dir).unwrap(),
            "manifest-removed" => std::fs::remove_file(model_dir.join("manifest.json")).unwrap(),
            "manifest-replaced" => {
                let replacement = test_artifact(b"ghijkl");
                std::fs::write(
                    model_dir.join("manifest.json"),
                    serde_json::to_vec_pretty(&exact_manifest("custom-model".into(), &replacement))
                        .unwrap(),
                )
                .unwrap();
            }
            _ => unreachable!(),
        }
        #[cfg(unix)]
        let before = model_dir
            .exists()
            .then(|| exact_directory_snapshot(&model_dir));
        let progress_calls = Cell::new(0);
        let capacity_calls = Cell::new(0);
        let token_calls = Cell::new(0);
        let download_calls = Cell::new(0);

        let result = transfer_selected_with(
            &service,
            TransferSelected::for_installed(artifact, "custom-model".into()),
            TransferControl::new(),
            |_| progress_calls.set(progress_calls.get() + 1),
            |_| {
                capacity_calls.set(capacity_calls.get() + 1);
                Ok((u64::MAX, 1))
            },
            || {
                token_calls.set(token_calls.get() + 1);
                None
            },
            |_, _, _, _, _| {
                download_calls.set(download_calls.get() + 1);
                Err(DownloadFailure::Durability)
            },
        );
        let error = match result {
            Ok(_) => panic!("stale inspected-installed {label} manifest must refuse"),
            Err(error) => error,
        };

        assert_eq!(error.kind(), TransferErrorKind::UnsafeLocalState, "{label}");
        assert_eq!(progress_calls.get(), 0, "{label}");
        assert_eq!(capacity_calls.get(), 0, "{label}");
        assert_eq!(token_calls.get(), 0, "{label}");
        assert_eq!(download_calls.get(), 0, "{label}");
        assert!(!model_dir.join("pending.json").exists(), "{label}");
        #[cfg(unix)]
        match before {
            Some(before) => assert_eq!(exact_directory_snapshot(&model_dir), before, "{label}"),
            None => assert!(!model_dir.exists(), "{label}"),
        }
    }
}

#[test]
fn inspected_installed_intent_retains_missing_and_corrupt_artifact_repair() {
    use std::cell::Cell;

    for (label, initial_bytes) in [("missing", None), ("corrupt", Some(b"ghijkl".as_slice()))] {
        let root = tempfile::tempdir().unwrap();
        let service = test_service(root.path());
        let artifact = test_artifact(b"abcdef");
        let model_dir = root.path().join("models/custom-model");
        let lock = crate::catalog::ModelLock::acquire_for_transfer(&model_dir).unwrap();
        drop(lock);
        std::fs::write(
            model_dir.join("manifest.json"),
            serde_json::to_vec_pretty(&exact_manifest("custom-model".into(), &artifact)).unwrap(),
        )
        .unwrap();
        if let Some(bytes) = initial_bytes {
            std::fs::write(model_dir.join("model.gguf"), bytes).unwrap();
        }
        let download_calls = Cell::new(0);

        let result = transfer_selected_with(
            &service,
            TransferSelected::for_installed(artifact, "custom-model".into()),
            TransferControl::new(),
            |_| {},
            |_| Ok((u64::MAX, 1)),
            || None,
            |_, destination, _, _, _| {
                download_calls.set(download_calls.get() + 1);
                let final_path = destination.join("model.gguf");
                std::fs::write(&final_path, b"abcdef").unwrap();
                Ok(DownloadTerminalOutcome::Complete(
                    crate::download::DownloadOutcome::Pulled(final_path),
                ))
            },
        )
        .unwrap_or_else(|error| panic!("{label} repair failed: {error:?}"));

        assert_eq!(result.model_id(), "custom-model", "{label}");
        assert_eq!(
            result.disposition(),
            TransferDisposition::AlreadyInstalled,
            "{label}"
        );
        assert_eq!(download_calls.get(), 1, "{label}");
        assert_eq!(
            std::fs::read(model_dir.join("model.gguf")).unwrap(),
            b"abcdef",
            "{label}"
        );
    }
}

#[test]
fn installed_completion_debris_uses_only_zero_requirement_revalidated_cleanup() {
    let root = tempfile::tempdir().unwrap();
    let service = test_service(root.path());
    let artifact = test_artifact(b"abcdef");
    let model_dir = root.path().join("models/demo");
    let lock = crate::catalog::ModelLock::acquire_for_transfer(&model_dir).unwrap();
    drop(lock);
    let manifest_bytes =
        serde_json::to_vec_pretty(&exact_manifest("demo".into(), &artifact)).unwrap();
    std::fs::write(model_dir.join("manifest.json"), &manifest_bytes).unwrap();
    std::fs::write(model_dir.join("pending.json"), &manifest_bytes).unwrap();
    std::fs::write(model_dir.join("pending.json.tmp"), b"pending temp").unwrap();
    std::fs::write(model_dir.join("manifest.json.tmp"), b"manifest temp").unwrap();
    std::fs::write(model_dir.join("model.gguf"), b"abcdef").unwrap();
    std::fs::write(model_dir.join("foreign"), b"keep").unwrap();

    let result = transfer_selected_with(
        &service,
        TransferSelected::new(artifact, Some("demo".into())),
        TransferControl::new(),
        |_| {},
        |_| panic!("zero-requirement completion cleanup must not probe capacity"),
        || panic!("completion cleanup must not look up token"),
        |_, _, _, _, _| panic!("completion cleanup must not call downloader"),
    )
    .unwrap();
    assert_eq!(result.disposition(), TransferDisposition::AlreadyInstalled);
    assert_eq!(
        std::fs::read(model_dir.join("manifest.json")).unwrap(),
        manifest_bytes
    );
    assert_eq!(
        std::fs::read(model_dir.join("model.gguf")).unwrap(),
        b"abcdef"
    );
    assert_eq!(std::fs::read(model_dir.join("foreign")).unwrap(), b"keep");
    for removed in ["pending.json", "pending.json.tmp", "manifest.json.tmp"] {
        assert!(!model_dir.join(removed).exists());
    }
}

#[cfg(unix)]
#[test]
fn installed_completion_cleanup_reaudits_exact_final_before_catalog_mutation() {
    for (mutated_name, mutated_bytes) in [
        ("model.gguf", b"ghijkl".as_slice()),
        ("model.gguf.invalid", b"broken".as_slice()),
    ] {
        let root = tempfile::tempdir().unwrap();
        let artifact = test_artifact(b"abcdef");
        let manifest = exact_manifest("demo".into(), &artifact);
        let model_dir = root.path().join("models/demo");
        let lock = crate::catalog::ModelLock::acquire_for_transfer(&model_dir).unwrap();
        let manifest_bytes = serde_json::to_vec_pretty(&manifest).unwrap();
        std::fs::write(model_dir.join("manifest.json"), &manifest_bytes).unwrap();
        std::fs::write(model_dir.join("pending.json"), &manifest_bytes).unwrap();
        std::fs::write(model_dir.join("pending.json.tmp"), b"pending temp").unwrap();
        std::fs::write(model_dir.join("manifest.json.tmp"), b"manifest temp").unwrap();
        std::fs::write(model_dir.join("model.gguf"), b"abcdef").unwrap();
        std::fs::write(model_dir.join("foreign"), b"keep").unwrap();

        let catalog_plan = crate::catalog::transfer::plan_transfer(&lock, &manifest);
        assert_eq!(
            catalog_plan.state(),
            crate::catalog::transfer::CatalogTransferState::InstalledCompletionDebris
        );
        let initial_artifact = match crate::download::plan::plan_artifact_transfer(
            lock.model_directory(),
            &model_dir,
            artifact.size(),
            artifact.sha256(),
            &|| false,
        ) {
            crate::download::plan::ArtifactPlanOutcome::Ready(plan) => plan,
            crate::download::plan::ArtifactPlanOutcome::Interrupted => {
                panic!("false control cannot interrupt the initial artifact audit")
            }
        };
        assert_eq!(
            initial_artifact.state(),
            crate::download::plan::ArtifactTransferState::ValidFinal
        );

        std::fs::write(model_dir.join(mutated_name), mutated_bytes).unwrap();
        let before = exact_directory_snapshot(&model_dir);
        let error = recover_installed_completion_after_artifact_revalidation(
            &lock,
            &model_dir,
            &manifest,
            &artifact,
            &catalog_plan,
        )
        .unwrap_err();
        assert_eq!(error.kind(), TransferErrorKind::UnsafeLocalState);
        assert_eq!(exact_directory_snapshot(&model_dir), before);
    }
}

#[test]
fn fresh_reentry_after_each_pending_staging_promotion_publication_and_temp_checkpoint_is_safe() {
    let root = tempfile::tempdir().unwrap();
    let artifact = test_artifact(b"abcdef");

    let paused = transfer_selected_with(
        &test_service(root.path()),
        TransferSelected::new(artifact.clone(), Some("resume".into())),
        TransferControl::new(),
        |_| {},
        |_| Ok((u64::MAX, 1)),
        || None,
        |_, model_dir, _, _, _| {
            std::fs::write(model_dir.join("model.gguf.part"), b"abc").unwrap();
            Ok(DownloadTerminalOutcome::Paused { retained_bytes: 3 })
        },
    )
    .unwrap();
    assert_eq!(paused.disposition(), TransferDisposition::Paused);
    let resume_dir = root.path().join("models/resume");
    std::fs::write(resume_dir.join("pending.json.tmp"), b"pending temp").unwrap();
    std::fs::write(resume_dir.join("manifest.json.tmp"), b"manifest temp").unwrap();

    let installed = transfer_selected_with(
        &test_service(root.path()),
        TransferSelected::new(artifact.clone(), Some("resume".into())),
        TransferControl::new(),
        |_| {},
        |_| Ok((u64::MAX, 1)),
        || None,
        |_, model_dir, _, _, _| {
            assert_eq!(
                std::fs::read(model_dir.join("model.gguf.part")).unwrap(),
                b"abc"
            );
            assert!(!model_dir.join("pending.json.tmp").exists());
            assert!(!model_dir.join("manifest.json.tmp").exists());
            std::fs::remove_file(model_dir.join("model.gguf.part")).unwrap();
            let final_path = model_dir.join("model.gguf");
            std::fs::write(&final_path, b"abcdef").unwrap();
            Ok(DownloadTerminalOutcome::Complete(
                crate::download::DownloadOutcome::Pulled(final_path),
            ))
        },
    )
    .unwrap();
    assert_eq!(installed.disposition(), TransferDisposition::Installed);
    assert!(resume_dir.join("manifest.json").exists());
    assert!(!resume_dir.join("pending.json").exists());

    let already = transfer_selected_with(
        &test_service(root.path()),
        TransferSelected::new(artifact.clone(), Some("resume".into())),
        TransferControl::new(),
        |_| {},
        |_| panic!("clean reentry must not probe"),
        || panic!("clean reentry must not look up a token"),
        |_, _, _, _, _| panic!("clean reentry must not call downloader"),
    )
    .unwrap();
    assert_eq!(already.disposition(), TransferDisposition::AlreadyInstalled);

    let promoted_dir = seed_pending(root.path(), "promoted", &artifact);
    std::fs::write(promoted_dir.join("model.gguf"), b"abcdef").unwrap();
    let promoted = transfer_selected_with(
        &test_service(root.path()),
        TransferSelected::new(artifact, Some("promoted".into())),
        TransferControl::new(),
        |_| {},
        |_| Ok((u64::MAX, 1)),
        || panic!("promoted reentry must not look up a token"),
        |_, _, _, _, _| panic!("promoted reentry must not call downloader"),
    )
    .unwrap();
    assert_eq!(promoted.disposition(), TransferDisposition::Installed);
    assert!(promoted_dir.join("manifest.json").exists());
    assert!(!promoted_dir.join("pending.json").exists());
}

#[test]
fn transfer_error_display_debug_and_diagnostics_redact_every_hostile_source() {
    let hostile = "https://evil.invalid/model?token=secret /private/model\u{1b}[31m";
    let artifact = crate::huggingface::test_resolved_file_for(
        "owner/evil.invalid?token=secret",
        "private-model.gguf",
        "a".repeat(64),
        7,
    );
    let errors = [
        TransferError::terminal(TransferErrorKind::UnsafeLocalState),
        TransferError::insufficient_disk(9, 3),
        TransferError::recovery(
            TransferErrorKind::Remote,
            "demo".into(),
            artifact.clone(),
            3,
            true,
        ),
        TransferError::recovery(
            TransferErrorKind::Publication,
            "demo".into(),
            artifact.clone(),
            7,
            false,
        ),
        download_failure(
            DownloadFailure::Legacy(hostile.into()),
            "demo".into(),
            artifact,
            false,
        ),
    ];
    for error in errors {
        for rendered in [error.to_string(), format!("{error:?}")] {
            assert!(!rendered.contains("secret"));
            assert!(!rendered.contains("evil.invalid"));
            assert!(!rendered.contains("/private/model"));
            assert!(!rendered.contains('\u{1b}'));
        }
    }
}

#[test]
fn prepare_discard_requires_existing_directory_lock_and_exact_remote_v1_pending_without_creation() {
    let root = tempfile::tempdir().unwrap();
    let service = test_service(root.path());
    let missing = root.path().join("models/missing");
    let error = match service.prepare_discard("missing".into()) {
        Ok(_) => panic!("missing directory must not produce a candidate"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), TransferErrorKind::NoIncompleteTransfer);
    assert!(!missing.exists());

    let unlocked = root.path().join("models/unlocked");
    std::fs::create_dir_all(&unlocked).unwrap();
    let error = match service.prepare_discard("unlocked".into()) {
        Ok(_) => panic!("missing lock must not produce a candidate"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), TransferErrorKind::NoIncompleteTransfer);
    assert!(!unlocked.join(".lock").exists());

    let artifact = test_artifact(b"abcdef");
    let model_dir = seed_pending(root.path(), "demo", &artifact);
    std::fs::write(model_dir.join("model.gguf.part"), b"abc").unwrap();
    let candidate = service.prepare_discard("demo".into()).unwrap();
    assert_eq!(candidate.model_id(), "demo");

    let held = crate::catalog::ModelLock::acquire_existing(&model_dir).unwrap();
    let error = match service.prepare_discard("demo".into()) {
        Ok(_) => panic!("busy transfer must not produce a candidate"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), TransferErrorKind::Busy);
    drop(held);
}

#[test]
fn discard_candidate_model_id_accessor_and_by_value_consume_signature_compile() {
    let _: fn(&DiscardCandidate) -> &str = DiscardCandidate::model_id;
    let _: fn(&AppService, DiscardCandidate) -> Result<(), TransferError> =
        AppService::discard_transfer;

    let root = tempfile::tempdir().unwrap();
    let service = test_service(root.path());
    let artifact = test_artifact(b"abcdef");
    seed_pending(root.path(), "demo", &artifact);
    let candidate = service.prepare_discard("demo".into()).unwrap();
    assert_eq!(candidate.model_id(), "demo");
    service.discard_transfer(candidate).unwrap();
}

#[test]
fn unchanged_candidate_removes_restart_then_part_syncs_then_pending_last_and_syncs() {
    let root = tempfile::tempdir().unwrap();
    let service = test_service(root.path());
    let artifact = test_artifact(b"abcdef");
    let model_dir = seed_pending(root.path(), "demo", &artifact);
    std::fs::write(model_dir.join("model.gguf.part"), b"abc").unwrap();
    std::fs::write(model_dir.join("model.gguf.part.restart"), b"abcd").unwrap();
    std::fs::write(model_dir.join("pending.json.tmp"), b"pending temp").unwrap();
    std::fs::write(model_dir.join("manifest.json.tmp"), b"manifest temp").unwrap();
    std::fs::write(model_dir.join("foreign"), b"keep").unwrap();

    let candidate = service.prepare_discard("demo".into()).unwrap();
    service.discard_transfer(candidate).unwrap();
    for removed in ["model.gguf.part.restart", "model.gguf.part", "pending.json"] {
        assert!(!model_dir.join(removed).exists());
    }
    assert!(model_dir.join(".lock").exists());
    assert_eq!(
        std::fs::read(model_dir.join("pending.json.tmp")).unwrap(),
        b"pending temp"
    );
    assert_eq!(
        std::fs::read(model_dir.join("manifest.json.tmp")).unwrap(),
        b"manifest temp"
    );
    assert_eq!(std::fs::read(model_dir.join("foreign")).unwrap(), b"keep");
}

#[test]
fn discard_reproves_final_and_invalid_absence_before_removing_pending_last() {
    for authority_name in ["model.gguf", "model.gguf.invalid"] {
        let root = tempfile::tempdir().unwrap();
        let artifact = test_artifact(b"abcdef");
        let model_dir = seed_pending(root.path(), "demo", &artifact);
        let part = model_dir.join("model.gguf.part");
        let restart = model_dir.join("model.gguf.part.restart");
        std::fs::write(&part, b"abc").unwrap();
        std::fs::write(&restart, b"abcd").unwrap();
        let lock = crate::catalog::ModelLock::acquire_existing(&model_dir).unwrap();
        let catalog = crate::catalog::transfer::plan_discard(&lock, "demo").unwrap();
        let artifact =
            crate::download::plan_artifact_discard(lock.model_directory(), &model_dir).unwrap();

        crate::download::discard_artifact_bytes(lock.model_directory(), &model_dir, artifact)
            .unwrap();
        assert!(!part.exists());
        assert!(!restart.exists());
        std::fs::write(model_dir.join(authority_name), b"new authority").unwrap();

        let error = finish_discard_after_artifact_bytes(&lock, &model_dir, catalog).unwrap_err();
        assert_eq!(error.kind(), TransferErrorKind::Durability);
        assert!(model_dir.join("pending.json").exists());
        assert_eq!(
            std::fs::read(model_dir.join(authority_name)).unwrap(),
            b"new authority"
        );
    }
}

#[test]
fn zero_byte_incomplete_transfer_discards_pending_and_keeps_directory_lock_and_temps() {
    let root = tempfile::tempdir().unwrap();
    let service = test_service(root.path());
    let artifact = test_artifact(b"abcdef");
    let model_dir = seed_pending(root.path(), "demo", &artifact);
    std::fs::write(model_dir.join("pending.json.tmp"), b"pending temp").unwrap();
    std::fs::write(model_dir.join("manifest.json.tmp"), b"manifest temp").unwrap();

    let candidate = service.prepare_discard("demo".into()).unwrap();
    service.discard_transfer(candidate).unwrap();
    assert!(!model_dir.join("pending.json").exists());
    assert!(model_dir.join(".lock").exists());
    assert_eq!(
        std::fs::read(model_dir.join("pending.json.tmp")).unwrap(),
        b"pending temp"
    );
    assert_eq!(
        std::fs::read(model_dir.join("manifest.json.tmp")).unwrap(),
        b"manifest temp"
    );
}

#[test]
fn discard_candidate_change_during_prompt_window_returns_incomplete_transfer_changed_without_deletion(
) {
    let root = tempfile::tempdir().unwrap();
    let service = test_service(root.path());
    let artifact = test_artifact(b"abcdef");
    let model_dir = seed_pending(root.path(), "demo", &artifact);
    let part = model_dir.join("model.gguf.part");
    let moved = model_dir.join("captured-part");
    std::fs::write(&part, b"abc").unwrap();
    std::fs::write(model_dir.join("model.gguf.part.restart"), b"abcd").unwrap();
    let candidate = service.prepare_discard("demo".into()).unwrap();
    std::fs::rename(&part, &moved).unwrap();
    std::fs::write(&part, b"XYZ").unwrap();

    let error = service.discard_transfer(candidate).unwrap_err();
    assert_eq!(error.kind(), TransferErrorKind::IncompleteTransferChanged);
    assert_eq!(std::fs::read(&moved).unwrap(), b"abc");
    assert_eq!(std::fs::read(&part).unwrap(), b"XYZ");
    assert_eq!(
        std::fs::read(model_dir.join("model.gguf.part.restart")).unwrap(),
        b"abcd"
    );
    assert!(model_dir.join("pending.json").exists());
}

#[test]
fn fresh_installed_preparation_returns_completion_won_with_no_rm_fallthrough() {
    let root = tempfile::tempdir().unwrap();
    let service = test_service(root.path());
    let artifact = test_artifact(b"abcdef");
    let model_dir = seed_pending(root.path(), "demo", &artifact);
    let manifest = serde_json::to_vec_pretty(&exact_manifest("demo".into(), &artifact)).unwrap();
    std::fs::write(model_dir.join("manifest.json"), &manifest).unwrap();
    std::fs::write(model_dir.join("model.gguf"), b"abcdef").unwrap();
    #[cfg(unix)]
    let before = exact_directory_snapshot(&model_dir);

    let error = match service.prepare_discard("demo".into()) {
        Ok(_) => panic!("installed completion must not produce a discard candidate"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), TransferErrorKind::CompletionWon);
    #[cfg(unix)]
    assert_eq!(exact_directory_snapshot(&model_dir), before);
}

#[cfg(unix)]
#[test]
fn busy_different_malformed_unsafe_local_bundle_final_manifest_invalid_and_foreign_states_delete_nothing(
) {
    let root = tempfile::tempdir().unwrap();
    let service = test_service(root.path());
    let artifact = test_artifact(b"abcdef");

    let busy_dir = seed_pending(root.path(), "busy", &artifact);
    std::fs::write(busy_dir.join("model.gguf.part"), b"abc").unwrap();
    let before = exact_directory_snapshot(&busy_dir);
    let held = crate::catalog::ModelLock::acquire_existing(&busy_dir).unwrap();
    let error = match service.prepare_discard("busy".into()) {
        Ok(_) => panic!("busy state must not produce a candidate"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), TransferErrorKind::Busy);
    assert_eq!(exact_directory_snapshot(&busy_dir), before);
    drop(held);

    for (model_id, mutate, expected) in [
        ("different", 0_u8, TransferErrorKind::ArtifactConflict),
        ("malformed", 1, TransferErrorKind::UnsafeLocalState),
        ("invalid", 2, TransferErrorKind::UnsafeLocalState),
        ("final", 3, TransferErrorKind::UnsafeLocalState),
        ("manifest-invalid", 4, TransferErrorKind::UnsafeLocalState),
        ("local-bundle", 5, TransferErrorKind::UnsafeLocalState),
    ] {
        let model_dir = seed_pending(root.path(), model_id, &artifact);
        match mutate {
            0 => {
                let different = crate::huggingface::test_resolved_file_for(
                    "other/repo",
                    "model.gguf",
                    "b".repeat(64),
                    6,
                );
                std::fs::write(
                    model_dir.join("pending.json"),
                    serde_json::to_vec_pretty(&exact_manifest("other".into(), &different)).unwrap(),
                )
                .unwrap();
            }
            1 => std::fs::write(model_dir.join("pending.json"), b"{").unwrap(),
            2 => std::fs::write(model_dir.join("model.gguf.invalid"), b"evidence").unwrap(),
            3 => std::fs::write(model_dir.join("model.gguf"), b"abcdef").unwrap(),
            4 => std::fs::write(model_dir.join("manifest.json"), b"not json").unwrap(),
            5 => {
                let mut local = exact_manifest(model_id.into(), &artifact);
                local.version = 2;
                local.repo = None;
                local.revision = None;
                local.remote_filename = None;
                local.origin = Some(crate::catalog::Origin::Local);
                local.source_filename = Some("model.gguf".into());
                std::fs::write(
                    model_dir.join("pending.json"),
                    serde_json::to_vec_pretty(&local).unwrap(),
                )
                .unwrap();
            }
            _ => unreachable!(),
        }
        let before = exact_directory_snapshot(&model_dir);
        let error = match service.prepare_discard(model_id.into()) {
            Ok(_) => panic!("{model_id} state must not produce a candidate"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), expected);
        assert_eq!(exact_directory_snapshot(&model_dir), before);
    }

    let foreign_dir = seed_pending(root.path(), "foreign", &artifact);
    std::fs::write(foreign_dir.join("model.gguf.part"), b"abc").unwrap();
    std::fs::write(foreign_dir.join("foreign.bin"), b"keep").unwrap();
    let candidate = service.prepare_discard("foreign".into()).unwrap();
    service.discard_transfer(candidate).unwrap();
    assert_eq!(
        std::fs::read(foreign_dir.join("foreign.bin")).unwrap(),
        b"keep"
    );
}

#[cfg(unix)]
#[test]
fn discard_refuses_symlink_hard_link_substitution_and_preserves_outside_witnesses() {
    use std::os::unix::fs::{symlink, MetadataExt};

    for (managed_name, use_symlink) in [
        ("model.gguf.part", true),
        ("model.gguf.part", false),
        ("model.gguf.part.restart", true),
        ("model.gguf.part.restart", false),
    ] {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let outside_path = outside.path().join("outside.bin");
        std::fs::write(&outside_path, b"outside").unwrap();
        let before = std::fs::metadata(&outside_path).unwrap();
        let service = test_service(root.path());
        let artifact = test_artifact(b"abcdef");
        let model_dir = seed_pending(root.path(), "demo", &artifact);
        if use_symlink {
            symlink(&outside_path, model_dir.join(managed_name)).unwrap();
        } else {
            std::fs::hard_link(&outside_path, model_dir.join(managed_name)).unwrap();
        }

        let error = match service.prepare_discard("demo".into()) {
            Ok(_) => panic!("unsafe managed entry must not produce a candidate"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), TransferErrorKind::UnsafeLocalState);
        let after = std::fs::metadata(&outside_path).unwrap();
        assert_eq!(std::fs::read(&outside_path).unwrap(), b"outside");
        assert_eq!((after.dev(), after.ino()), (before.dev(), before.ino()));
        assert_eq!(after.nlink(), if use_symlink { 1 } else { 2 });
        assert!(model_dir.join("pending.json").exists());
    }

    for (managed_name, use_symlink) in [
        ("model.gguf.part", true),
        ("model.gguf.part.restart", false),
    ] {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let outside_path = outside.path().join("outside.bin");
        std::fs::write(&outside_path, b"XYZ").unwrap();
        let service = test_service(root.path());
        let artifact = test_artifact(b"abcdef");
        let model_dir = seed_pending(root.path(), "demo", &artifact);
        let managed = model_dir.join(managed_name);
        let moved = model_dir.join(format!("captured-{}", managed_name.replace('.', "-")));
        std::fs::write(&managed, b"abc").unwrap();
        let candidate = service.prepare_discard("demo".into()).unwrap();
        std::fs::rename(&managed, &moved).unwrap();
        if use_symlink {
            symlink(&outside_path, &managed).unwrap();
        } else {
            std::fs::hard_link(&outside_path, &managed).unwrap();
        }

        let error = service.discard_transfer(candidate).unwrap_err();
        assert_eq!(error.kind(), TransferErrorKind::IncompleteTransferChanged);
        assert_eq!(std::fs::read(&moved).unwrap(), b"abc");
        assert_eq!(std::fs::read(&outside_path).unwrap(), b"XYZ");
        assert!(managed.exists());
        assert!(model_dir.join("pending.json").exists());
    }
}

#[test]
fn discard_failure_after_each_mutation_never_claims_success_and_reenters_safely() {
    for (remove_restart, remove_part) in [(true, false), (false, true), (true, true)] {
        let root = tempfile::tempdir().unwrap();
        let service = test_service(root.path());
        let artifact = test_artifact(b"abcdef");
        let model_dir = seed_pending(root.path(), "demo", &artifact);
        let part = model_dir.join("model.gguf.part");
        let restart = model_dir.join("model.gguf.part.restart");
        std::fs::write(&part, b"abc").unwrap();
        std::fs::write(&restart, b"abcd").unwrap();
        let candidate = service.prepare_discard("demo".into()).unwrap();
        if remove_restart {
            std::fs::remove_file(&restart).unwrap();
        }
        if remove_part {
            std::fs::remove_file(&part).unwrap();
        }

        let error = service.discard_transfer(candidate).unwrap_err();
        assert_eq!(error.kind(), TransferErrorKind::IncompleteTransferChanged);
        assert!(model_dir.join("pending.json").exists());
        let fresh = service.prepare_discard("demo".into()).unwrap();
        service.discard_transfer(fresh).unwrap();
        assert!(!model_dir.join("pending.json").exists());
        assert!(!part.exists());
        assert!(!restart.exists());
    }

    let root = tempfile::tempdir().unwrap();
    let service = test_service(root.path());
    let artifact = test_artifact(b"abcdef");
    let model_dir = seed_pending(root.path(), "demo", &artifact);
    let candidate = service.prepare_discard("demo".into()).unwrap();
    std::fs::remove_file(model_dir.join("pending.json")).unwrap();
    let error = service.discard_transfer(candidate).unwrap_err();
    assert_eq!(error.kind(), TransferErrorKind::IncompleteTransferChanged);
    let error = match service.prepare_discard("demo".into()) {
        Ok(_) => panic!("completed discard must not produce another candidate"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), TransferErrorKind::NoIncompleteTransfer);
}
