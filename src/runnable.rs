use crate::catalog::Manifest;
use crate::paths::AppPaths;
use crate::runtime_fingerprint::RuntimeFingerprint;
use crate::{catalog, cli, config, runner, verification};
use std::path::Path;
use std::time::Instant;

pub(crate) struct Runnable {
    _model_lock: catalog::ModelLock,
    launch: runner::Launch,
    fingerprint: RuntimeFingerprint,
    allow_primary_fallback: bool,
}

pub(crate) struct PersistentFingerprintCandidates(Vec<RuntimeFingerprint>);

struct ManagedPersistentPlan {
    ctx: u32,
    profile: runner::LaunchProfile,
    candidates: PersistentFingerprintCandidates,
}

enum ManagedPersistentPlanError {
    Config(String),
    Model(String),
}

impl ManagedPersistentPlanError {
    fn into_message(self) -> String {
        match self {
            Self::Config(message) | Self::Model(message) => message,
        }
    }
}

impl PersistentFingerprintCandidates {
    pub(crate) fn as_slice(&self) -> &[RuntimeFingerprint] {
        &self.0
    }
}

impl Runnable {
    fn new(
        model_lock: catalog::ModelLock,
        launch: runner::Launch,
        fingerprint: RuntimeFingerprint,
        allow_primary_fallback: bool,
    ) -> Self {
        Self {
            _model_lock: model_lock,
            launch,
            fingerprint,
            allow_primary_fallback,
        }
    }

    pub(crate) fn launch(&self) -> &runner::Launch {
        &self.launch
    }

    pub(crate) fn fingerprint(&self) -> &RuntimeFingerprint {
        &self.fingerprint
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        model_lock: catalog::ModelLock,
        launch: runner::Launch,
        fingerprint: RuntimeFingerprint,
    ) -> Self {
        Self::new(model_lock, launch, fingerprint, true)
    }

    #[cfg(test)]
    pub(crate) fn without_primary_fallback_for_test(mut self) -> Self {
        self.allow_primary_fallback = false;
        self
    }

    pub(crate) fn primary_only(&mut self) -> Option<()> {
        if !self.allow_primary_fallback {
            return None;
        }
        let launch = self.launch.primary_only()?;
        let fingerprint = self.fingerprint.primary_only()?;
        self.launch = launch;
        self.fingerprint = fingerprint;
        Some(())
    }

    pub(crate) fn primary_only_for_service(&mut self) -> Option<()> {
        if !self.allow_primary_fallback {
            return None;
        }
        let launch = self.launch.primary_only()?;
        let fingerprint = self.fingerprint.primary_only_for_service()?;
        self.launch = launch;
        self.fingerprint = fingerprint;
        Some(())
    }
}

enum AdmissionSource {
    Installed(Manifest),
    Local(catalog::local::Candidate),
}

pub(crate) enum ManagedRunnableError {
    Conflict,
    Cancelled,
    ModelUnavailable(String),
    StartupFailed(String),
}

impl ManagedRunnableError {
    #[cfg(test)]
    fn into_message(self) -> String {
        match self {
            Self::Conflict => "model is busy in another Loxa command".into(),
            Self::Cancelled => "model admission was cancelled".into(),
            Self::ModelUnavailable(message) | Self::StartupFailed(message) => message,
        }
    }
}

pub(crate) fn resolve_runnable(
    id: String,
    runtime: cli::RuntimeArgs,
    paths: &AppPaths,
) -> Result<Runnable, String> {
    let config = config::load(&paths.config)?;
    let ctx = config::resolve_value(runtime.ctx, config.ctx, 4096);
    let port = config::resolve_value(runtime.port, config.port, 0);
    let installed = catalog::load_reconciled_catalog(&paths.models)?;
    let installed = installed.into_iter().find(|entry| entry.id == id);
    let (source, profile, server) = match installed {
        Some(manifest) => {
            let profile = launch_profile(&manifest, &paths.models, paths.runtime_identity)?;
            let server = runner::discover_from_process(
                runtime.server.as_deref(),
                &paths.managed_server,
                &profile,
                paths.runtime_identity,
            )?;
            (AdmissionSource::Installed(manifest), profile, server)
        }
        None => {
            let candidate = catalog::local::discover(&paths.models)?
                .into_iter()
                .find(|candidate| candidate.id == id)
                .ok_or_else(|| format!("unknown model id {id}"))?;
            let profile = runner::LaunchProfile::generic();
            let server = runner::discover_from_process(
                runtime.server.as_deref(),
                &paths.managed_server,
                &profile,
                paths.runtime_identity,
            )?;
            (AdmissionSource::Local(candidate), profile, server)
        }
    };
    let admission_started = Instant::now();
    let (manifest, model_lock, admission) = match source {
        AdmissionSource::Local(candidate) => {
            let catalog::local::CapturedAdoption {
                manifest,
                model_lock,
                verified,
            } = catalog::local::adopt_captured(&paths.models, &candidate)?;
            let artifact = manifest.artifact_path(&paths.models);
            verification::refresh_verified(&model_lock, &manifest, &artifact, &verified, None)?;
            (manifest, model_lock, verification::Admission::Verified)
        }
        AdmissionSource::Installed(manifest) => {
            let (model_lock, admission) = admit_installed(&manifest, paths)?;
            (manifest, model_lock, admission)
        }
    };
    report_admission(&manifest.id, admission, admission_started);
    let artifact = manifest.artifact_path(&paths.models);
    let policy = runner::LaunchPolicy::Foreground;
    let fingerprint = RuntimeFingerprint::from_manifest(
        &manifest,
        ctx,
        profile.effective_profile(),
        policy.sleep_idle_seconds(),
    )?;
    Ok(Runnable::new(
        model_lock,
        runner::Launch {
            server,
            managed_runtime: None,
            model: artifact,
            id: manifest.id,
            requested_port: port,
            ctx,
            profile,
            policy,
        },
        fingerprint,
        !paths.runtime_identity.is_bundled(),
    ))
}

#[cfg(test)]
pub(crate) fn resolve_managed_runnable(
    manifest: Manifest,
    paths: &AppPaths,
) -> Result<Runnable, String> {
    resolve_managed_runnable_for_host(manifest, paths, &|| false)
        .map_err(ManagedRunnableError::into_message)
}

pub(crate) fn resolve_managed_runnable_for_host(
    manifest: Manifest,
    paths: &AppPaths,
    cancelled: &impl Fn() -> bool,
) -> Result<Runnable, ManagedRunnableError> {
    resolve_managed_runnable_with_admission(manifest, paths, |manifest, paths| {
        admit_installed_for_host(manifest, paths, cancelled)
    })
}

pub(crate) fn resolve_managed_runnable_for_service(
    manifest: Manifest,
    paths: &AppPaths,
    cancelled: &impl Fn() -> bool,
) -> Result<Runnable, ManagedRunnableError> {
    let installed =
        catalog::load_catalog(&paths.models).map_err(ManagedRunnableError::ModelUnavailable)?;
    if !installed.iter().any(|candidate| candidate == &manifest) {
        return Err(ManagedRunnableError::ModelUnavailable(format!(
            "model {} is not installed",
            manifest.id
        )));
    }
    let config = config::load(&paths.config).map_err(ManagedRunnableError::StartupFailed)?;
    let ctx = config::resolve_value(None, config.ctx, 4096);
    let profile = launch_profile(&manifest, &paths.models, paths.runtime_identity)
        .map_err(ManagedRunnableError::ModelUnavailable)?;
    let server =
        runner::validate_managed_runtime(paths).map_err(ManagedRunnableError::StartupFailed)?;
    let admission_started = Instant::now();
    let (model_lock, admission) = admit_installed_for_host(&manifest, paths, cancelled)?;
    report_admission(&manifest.id, admission, admission_started);
    let fingerprint =
        RuntimeFingerprint::from_manifest_for_service(&manifest, ctx, profile.effective_profile())
            .map_err(ManagedRunnableError::ModelUnavailable)?;
    let artifact = manifest.artifact_path(&paths.models);
    let server_path = server.source_server().to_path_buf();
    Ok(Runnable::new(
        model_lock,
        runner::Launch {
            server: server_path,
            managed_runtime: Some(server),
            model: artifact,
            id: manifest.id,
            requested_port: 0,
            ctx,
            profile,
            policy: runner::LaunchPolicy::Service,
        },
        fingerprint,
        !paths.runtime_identity.is_bundled(),
    ))
}

fn resolve_managed_runnable_with_admission(
    manifest: Manifest,
    paths: &AppPaths,
    admit: impl FnOnce(
        &Manifest,
        &AppPaths,
    )
        -> Result<(catalog::ModelLock, verification::Admission), ManagedRunnableError>,
) -> Result<Runnable, ManagedRunnableError> {
    let installed =
        catalog::load_catalog(&paths.models).map_err(ManagedRunnableError::ModelUnavailable)?;
    if !installed.iter().any(|candidate| candidate == &manifest) {
        return Err(ManagedRunnableError::ModelUnavailable(format!(
            "model {} is not installed",
            manifest.id
        )));
    }

    let plan = managed_persistent_plan(&manifest, paths).map_err(|error| match error {
        ManagedPersistentPlanError::Config(message) => ManagedRunnableError::StartupFailed(message),
        ManagedPersistentPlanError::Model(message) => {
            ManagedRunnableError::ModelUnavailable(message)
        }
    })?;
    let server =
        runner::validate_managed_runtime(paths).map_err(ManagedRunnableError::StartupFailed)?;
    let admission_started = Instant::now();
    let (model_lock, admission) = admit(&manifest, paths)?;
    report_admission(&manifest.id, admission, admission_started);
    let artifact = manifest.artifact_path(&paths.models);
    let policy = runner::LaunchPolicy::PersistentApp;
    let ManagedPersistentPlan {
        ctx,
        profile,
        candidates,
    } = plan;
    let fingerprint = candidates
        .0
        .into_iter()
        .next()
        .expect("managed persistent plan always has an exact fingerprint");
    let server_path = server.source_server().to_path_buf();
    Ok(Runnable::new(
        model_lock,
        runner::Launch {
            server: server_path,
            managed_runtime: Some(server),
            model: artifact,
            id: manifest.id,
            requested_port: 0,
            ctx,
            profile,
            policy,
        },
        fingerprint,
        !paths.runtime_identity.is_bundled(),
    ))
}

#[cfg(test)]
fn resolve_managed_runnable_with_test_admission(
    manifest: Manifest,
    paths: &AppPaths,
) -> Result<Runnable, ManagedRunnableError> {
    resolve_managed_runnable_with_admission(manifest, paths, |manifest, paths| {
        let model_dir = paths
            .model_dir(&manifest.id)
            .map_err(ManagedRunnableError::ModelUnavailable)?;
        let model_lock =
            catalog::ModelLock::acquire_existing(&model_dir).map_err(|error| match error {
                catalog::ModelLockError::Busy => ManagedRunnableError::Conflict,
                catalog::ModelLockError::Missing | catalog::ModelLockError::UnsafeLocalState => {
                    ManagedRunnableError::ModelUnavailable(
                        "installed model state is unavailable".into(),
                    )
                }
            })?;
        Ok((model_lock, verification::Admission::ReceiptHit))
    })
}

pub(crate) fn expected_persistent_fingerprints(
    manifest: &Manifest,
    paths: &AppPaths,
) -> Result<PersistentFingerprintCandidates, String> {
    managed_persistent_plan(manifest, paths)
        .map(|plan| plan.candidates)
        .map_err(ManagedPersistentPlanError::into_message)
}

fn managed_persistent_plan(
    manifest: &Manifest,
    paths: &AppPaths,
) -> Result<ManagedPersistentPlan, ManagedPersistentPlanError> {
    let config = config::load(&paths.config).map_err(ManagedPersistentPlanError::Config)?;
    let ctx = config::resolve_value(None, config.ctx, 4096);
    let profile = launch_profile(manifest, &paths.models, paths.runtime_identity)
        .map_err(ManagedPersistentPlanError::Model)?;
    let fingerprint = RuntimeFingerprint::from_manifest(
        manifest,
        ctx,
        profile.effective_profile(),
        runner::LaunchPolicy::PersistentApp.sleep_idle_seconds(),
    )
    .map_err(ManagedPersistentPlanError::Model)?;
    let fallback = (!paths.runtime_identity.is_bundled())
        .then(|| fingerprint.primary_only())
        .flatten();
    let mut candidates = vec![fingerprint];
    candidates.extend(fallback);
    Ok(ManagedPersistentPlan {
        ctx,
        profile,
        candidates: PersistentFingerprintCandidates(candidates),
    })
}

fn admit_installed(
    manifest: &Manifest,
    paths: &AppPaths,
) -> Result<(catalog::ModelLock, verification::Admission), String> {
    let model_dir = paths.model_dir(&manifest.id)?;
    let artifact = manifest.artifact_path(&paths.models);
    let draft = manifest
        .draft_artifact()
        .map(|draft| paths.models.join(&manifest.id).join(draft.local_filename));
    let model_lock = catalog::ModelLock::acquire(&model_dir)?;
    let admission = verification::verify_or_refresh(
        &model_lock,
        &model_dir,
        manifest,
        &artifact,
        draft.as_deref(),
        || verification::verify_artifacts(manifest, &artifact, draft.as_deref()),
    )?;
    Ok((model_lock, admission))
}

fn admit_installed_for_host(
    manifest: &Manifest,
    paths: &AppPaths,
    cancelled: &impl Fn() -> bool,
) -> Result<(catalog::ModelLock, verification::Admission), ManagedRunnableError> {
    let model_dir = paths
        .model_dir(&manifest.id)
        .map_err(ManagedRunnableError::ModelUnavailable)?;
    let artifact = manifest.artifact_path(&paths.models);
    let draft = manifest
        .draft_artifact()
        .map(|draft| paths.models.join(&manifest.id).join(draft.local_filename));
    let model_lock =
        catalog::ModelLock::acquire_existing(&model_dir).map_err(|error| match error {
            catalog::ModelLockError::Busy => ManagedRunnableError::Conflict,
            catalog::ModelLockError::Missing | catalog::ModelLockError::UnsafeLocalState => {
                ManagedRunnableError::ModelUnavailable(
                    "installed model state is unavailable".into(),
                )
            }
        })?;
    let admission = verification::verify_or_refresh_cancellable(
        &model_lock,
        &model_dir,
        manifest,
        &artifact,
        draft.as_deref(),
        cancelled,
    )
    .map_err(ManagedRunnableError::ModelUnavailable)?;
    let admission = match admission {
        verification::CancellableAdmission::Admitted(admission) => admission,
        verification::CancellableAdmission::Cancelled => {
            return Err(ManagedRunnableError::Cancelled)
        }
    };
    Ok((model_lock, admission))
}

fn report_admission(id: &str, admission: verification::Admission, started: Instant) {
    tracing::info!(
        event = "model_admission_complete",
        model_id = %id,
        result = match admission {
            verification::Admission::ReceiptHit => "receipt_hit",
            verification::Admission::Verified => "verified",
        },
        elapsed_ms = started.elapsed().as_millis() as u64,
    );
}

fn launch_profile(
    manifest: &Manifest,
    models_root: &Path,
    runtime_identity: crate::runtime_identity::RuntimeIdentity,
) -> Result<runner::LaunchProfile, String> {
    match (
        manifest.version,
        manifest.profile.as_deref(),
        manifest.runtime.as_ref(),
    ) {
        (3, Some(catalog::GEMMA4_MTP_PROFILE), Some(_runtime))
            if runtime_identity.supports_manifest(manifest) =>
        {
            Ok(runner::LaunchProfile::gemma4_mtp(
                manifest.draft_path(models_root),
            ))
        }
        #[cfg(test)]
        (3, Some(catalog::TEST_MTP_PROFILE), Some(runtime))
            if runtime.engine == "llama.cpp" && runtime.build == catalog::TEST_LLAMA_BUILD =>
        {
            Ok(runner::LaunchProfile::gemma4_mtp_for_test(
                manifest.draft_path(models_root),
                runtime.build.clone(),
            ))
        }
        (1 | 2, None, None) => Ok(runner::LaunchProfile::generic()),
        _ => Err("unsupported validated runtime profile".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Artifact, ArtifactProvenance, ArtifactRole, Manifest, ModelLock};
    use crate::download;
    use crate::runtime_fingerprint::{EffectiveProfile, RuntimeFingerprint};
    use crate::verification::{refresh_verified, verify_or_refresh, VerifiedArtifacts};
    use sha2::{Digest, Sha256};
    use std::cell::Cell;
    use std::ffi::OsStr;
    use std::fmt::Write as _;
    use std::path::Path;
    use tempfile::tempdir;

    fn manifest() -> Manifest {
        Manifest {
            version: 1,
            id: "demo".into(),
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
        }
    }

    fn bundle_manifest() -> Manifest {
        let mut manifest = manifest();
        manifest.version = 3;
        manifest.repo = None;
        manifest.revision = None;
        manifest.remote_filename = None;
        manifest.artifacts = Some(vec![
            Artifact {
                role: ArtifactRole::Model,
                local_filename: "model.gguf".into(),
                sha256: manifest.sha256.clone(),
                size: manifest.size,
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
        ]);
        manifest
    }

    fn production_bundle_manifest(build: &str) -> Manifest {
        Manifest {
            version: 3,
            id: "gemma-4-12b-it-qat-ud-q4-k-xl".into(),
            repo: None,
            revision: None,
            remote_filename: None,
            origin: None,
            source_filename: None,
            local_filename: "model.gguf".into(),
            sha256: catalog::GEMMA4_MODEL_SHA256.into(),
            size: catalog::GEMMA4_MODEL_SIZE,
            artifacts: Some(vec![
                Artifact {
                    role: ArtifactRole::Model,
                    local_filename: "model.gguf".into(),
                    sha256: catalog::GEMMA4_MODEL_SHA256.into(),
                    size: catalog::GEMMA4_MODEL_SIZE,
                    provenance: ArtifactProvenance::Local {
                        source_filename: "target.gguf".into(),
                    },
                },
                Artifact {
                    role: ArtifactRole::Draft,
                    local_filename: "draft.gguf".into(),
                    sha256: catalog::GEMMA4_DRAFT_SHA256.into(),
                    size: catalog::GEMMA4_DRAFT_SIZE,
                    provenance: ArtifactProvenance::Local {
                        source_filename: "draft-source.gguf".into(),
                    },
                },
            ]),
            profile: Some(catalog::GEMMA4_MTP_PROFILE.into()),
            runtime: Some(catalog::RuntimeQualification {
                engine: "llama.cpp".into(),
                build: build.into(),
            }),
        }
    }

    fn install_manifest_metadata(paths: &AppPaths, manifest: &Manifest) {
        let model_dir = paths.model_dir(&manifest.id).unwrap();
        std::fs::create_dir_all(&model_dir).unwrap();
        let mut bytes = serde_json::to_vec_pretty(manifest).unwrap();
        bytes.push(b'\n');
        std::fs::write(model_dir.join("manifest.json"), bytes).unwrap();
        drop(ModelLock::acquire(&model_dir).unwrap());
    }

    fn assert_active_version(server: &Path, expected: &str) {
        let output = std::process::Command::new(server)
            .arg("--version")
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        let combined = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            combined.lines().filter(|line| *line == expected).count(),
            1,
            "{combined}"
        );
    }

    fn assert_production_mutations_rejected(paths: &AppPaths, exact: &Manifest) {
        let mut changed_build = exact.clone();
        changed_build.runtime.as_mut().unwrap().build = "b10122".into();
        let mut changed_profile = exact.clone();
        changed_profile.profile = Some("gemma4-mtp-v2".into());
        let mut changed_artifact = exact.clone();
        changed_artifact.artifacts.as_mut().unwrap()[1].sha256 = "0".repeat(64);

        for changed in [changed_build, changed_profile, changed_artifact] {
            install_manifest_metadata(paths, &changed);
            assert!(matches!(
                resolve_managed_runnable_with_test_admission(changed, paths),
                Err(ManagedRunnableError::ModelUnavailable(_))
            ));
        }
    }

    #[cfg(unix)]
    #[test]
    fn b10344_provenance_under_legacy_cli_selects_and_reports_active_b10121_without_rewrite() {
        let root = tempdir().unwrap();
        let paths = AppPaths::from_values(Some(root.path()), None).unwrap();
        let manifest = production_bundle_manifest(catalog::GEMMA4_BUNDLED_LLAMA_BUILD);
        install_manifest_metadata(&paths, &manifest);
        install_managed_server(&paths);

        let mut runnable =
            match resolve_managed_runnable_with_test_admission(manifest.clone(), &paths) {
                Ok(runnable) => runnable,
                Err(error) => panic!("{}", error.into_message()),
            };

        assert_eq!(
            paths.runtime_identity,
            crate::runtime_identity::RuntimeIdentity::LegacyCliB10121
        );
        assert_eq!(runnable.launch().server, paths.managed_server);
        assert_eq!(
            paths.runtime_identity.version_line(),
            "version: 10121 (555881ebc)"
        );
        assert_active_version(
            &runnable.launch().server,
            paths.runtime_identity.version_line(),
        );
        assert_eq!(
            runnable.fingerprint().effective_profile(),
            EffectiveProfile::Gemma4Mtp
        );
        assert!(runnable.primary_only().is_some());
        drop(runnable);
        assert_eq!(
            catalog::load_catalog(&paths.models).unwrap()[0]
                .runtime
                .as_ref()
                .unwrap()
                .build,
            catalog::GEMMA4_BUNDLED_LLAMA_BUILD
        );
        assert_production_mutations_rejected(&paths, &manifest);
    }

    #[cfg(target_os = "macos")]
    #[test]
    #[ignore = "requires the finalized built app"]
    fn legacy_provenance_under_bundled_app_selects_and_reports_active_b10344_without_rewrite() {
        let app = std::path::PathBuf::from(std::env::var_os("LOXA_BUILT_APP").unwrap());
        let root = tempdir().unwrap();
        let paths = AppPaths::from_application_values(
            &app.join("Contents/MacOS/loxa-app"),
            Some(root.path()),
            None,
        )
        .unwrap();
        let manifest = production_bundle_manifest(catalog::GEMMA4_LEGACY_LLAMA_BUILD);
        install_manifest_metadata(&paths, &manifest);

        let mut runnable =
            match resolve_managed_runnable_with_test_admission(manifest.clone(), &paths) {
                Ok(runnable) => runnable,
                Err(error) => panic!("{}", error.into_message()),
            };

        assert_eq!(
            paths.runtime_identity,
            crate::runtime_identity::RuntimeIdentity::BundledB10344
        );
        assert_eq!(
            runnable.launch().server,
            app.join("Contents/MacOS/llama-server")
        );
        assert_eq!(
            paths.runtime_identity.version_line(),
            "version: 10344 (7a20b417f)"
        );
        assert_active_version(
            &runnable.launch().server,
            paths.runtime_identity.version_line(),
        );
        assert_eq!(
            runnable.fingerprint().effective_profile(),
            EffectiveProfile::Gemma4Mtp
        );
        assert!(runnable.primary_only().is_none());
        drop(runnable);
        assert_eq!(
            catalog::load_catalog(&paths.models).unwrap()[0]
                .runtime
                .as_ref()
                .unwrap()
                .build,
            catalog::GEMMA4_LEGACY_LLAMA_BUILD
        );
        assert_production_mutations_rejected(&paths, &manifest);
    }

    fn verified_file(path: &Path) -> download::VerifiedRegularFile {
        let bytes = std::fs::read(path).unwrap();
        let mut checksum = String::with_capacity(64);
        for byte in Sha256::digest(&bytes) {
            write!(&mut checksum, "{byte:02x}").unwrap();
        }
        download::verify_regular_captured(path, bytes.len() as u64, &checksum).unwrap()
    }

    fn verified_artifacts(primary: &Path, draft: Option<&Path>) -> VerifiedArtifacts {
        VerifiedArtifacts {
            primary: verified_file(primary),
            draft: draft.map(verified_file),
        }
    }

    #[cfg(unix)]
    fn install_manifest(paths: &AppPaths) -> Manifest {
        install_exact_manifest(paths, manifest())
    }

    #[cfg(unix)]
    fn install_exact_manifest(paths: &AppPaths, manifest: Manifest) -> Manifest {
        let model_dir = paths.model_dir(&manifest.id).unwrap();
        std::fs::create_dir_all(&model_dir).unwrap();
        std::fs::write(model_dir.join("model.gguf"), b"abc").unwrap();
        catalog::publish_manifest(&paths.models, &manifest).unwrap();
        drop(ModelLock::acquire(&model_dir).unwrap());
        manifest
    }

    #[cfg(unix)]
    fn install_managed_server(paths: &AppPaths) {
        use std::os::unix::fs::PermissionsExt;

        std::fs::create_dir_all(paths.managed_server.parent().unwrap()).unwrap();
        std::fs::write(
            &paths.managed_server,
            b"#!/bin/sh\nprintf 'version: 10121 (555881ebc)\\n' >&2\n",
        )
        .unwrap();
        std::fs::set_permissions(
            &paths.managed_server,
            std::fs::Permissions::from_mode(0o700),
        )
        .unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn managed_host_admission_does_not_recreate_a_removed_installed_model() {
        let root = tempdir().unwrap();
        let paths = AppPaths::from_values(Some(root.path()), None).unwrap();
        let manifest = install_manifest(&paths);
        let model_dir = paths.model_dir(&manifest.id).unwrap();

        // This is the removal race after the host's final catalog snapshot.
        std::fs::remove_dir_all(&model_dir).unwrap();

        let result = admit_installed_for_host(&manifest, &paths, &|| false);

        assert!(matches!(
            result,
            Err(ManagedRunnableError::ModelUnavailable(_))
        ));
        let error = std::fs::symlink_metadata(&model_dir)
            .expect_err("host admission recreated the removed model directory");
        assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
    }

    #[cfg(unix)]
    #[test]
    fn managed_admission_uses_only_installed_model_managed_runtime_and_persistent_config() {
        let root = tempdir().unwrap();
        let paths = AppPaths::from_values(Some(root.path()), None).unwrap();
        let manifest = install_manifest(&paths);
        install_managed_server(&paths);
        std::fs::write(&paths.config, br#"{"version":1,"ctx":8192,"port":43123}"#).unwrap();

        let _: fn(Manifest, &AppPaths) -> Result<Runnable, String> = resolve_managed_runnable;
        let runnable = resolve_managed_runnable(manifest.clone(), &paths).unwrap();

        assert_eq!(runnable.launch().server, paths.managed_server);
        assert_eq!(runnable.launch().ctx, 8192);
        assert_eq!(runnable.launch().requested_port, 0);
        assert_eq!(
            runnable.launch().policy,
            runner::LaunchPolicy::PersistentApp
        );
        assert_eq!(runnable.fingerprint().sleep_policy(), Some(60));
        drop(runnable);

        std::fs::remove_file(&paths.config).unwrap();
        let defaults = resolve_managed_runnable(manifest, &paths).unwrap();
        assert_eq!(defaults.launch().ctx, 4096);
        assert_eq!(defaults.launch().requested_port, 0);
    }

    #[cfg(unix)]
    #[test]
    fn managed_admission_preserves_configured_zero_context() {
        let root = tempdir().unwrap();
        let paths = AppPaths::from_values(Some(root.path()), None).unwrap();
        let manifest = install_manifest(&paths);
        install_managed_server(&paths);
        std::fs::write(&paths.config, br#"{"version":1,"ctx":0}"#).unwrap();

        let runnable = resolve_managed_runnable(manifest, &paths).unwrap();
        let fingerprint = serde_json::to_value(runnable.fingerprint()).unwrap();

        assert_eq!(runnable.launch().ctx, 0);
        assert_eq!(fingerprint["effective_context"], 0);
    }

    #[cfg(unix)]
    #[test]
    fn foreground_admission_preserves_cli_zero_context() {
        let root = tempdir().unwrap();
        let paths = AppPaths::from_values(Some(root.path()), None).unwrap();
        let manifest = install_manifest(&paths);
        install_managed_server(&paths);

        let runnable = resolve_runnable(
            manifest.id,
            cli::RuntimeArgs {
                ctx: Some(0),
                port: None,
                server: Some(paths.managed_server.clone()),
            },
            &paths,
        )
        .unwrap();
        let fingerprint = serde_json::to_value(runnable.fingerprint()).unwrap();

        assert_eq!(runnable.launch().ctx, 0);
        assert_eq!(fingerprint["effective_context"], 0);
    }

    #[cfg(unix)]
    #[test]
    fn managed_admission_canonicalizes_an_uppercase_manifest_digest() {
        let root = tempdir().unwrap();
        let paths = AppPaths::from_values(Some(root.path()), None).unwrap();
        let mut manifest = manifest();
        manifest.sha256.make_ascii_uppercase();
        let manifest = install_exact_manifest(&paths, manifest);
        install_managed_server(&paths);

        let runnable = resolve_managed_runnable(manifest, &paths).unwrap();
        let fingerprint = serde_json::to_value(runnable.fingerprint()).unwrap();

        assert_eq!(
            fingerprint["primary"]["sha256"],
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[cfg(unix)]
    #[test]
    fn managed_admission_ignores_environment_and_path_runtime_candidates() {
        use std::os::unix::fs::PermissionsExt;

        let root = tempdir().unwrap();
        let paths = AppPaths::from_values(Some(root.path()), None).unwrap();
        install_manifest(&paths);
        install_managed_server(&paths);
        let environment = root.path().join("environment-server");
        let path_dir = root.path().join("path-bin");
        let path_server = path_dir.join("llama-server");
        std::fs::create_dir(&path_dir).unwrap();
        for candidate in [&environment, &path_server] {
            std::fs::write(
                candidate,
                b"#!/bin/sh\nprintf 'version: alternative\\n' >&2\n",
            )
            .unwrap();
            std::fs::set_permissions(candidate, std::fs::Permissions::from_mode(0o700)).unwrap();
        }

        let executable = std::env::current_exe().unwrap();
        let cases = [
            (Some(environment.as_os_str()), OsStr::new("")),
            (None, path_dir.as_os_str()),
        ];
        for (environment, path) in cases {
            let mut child = std::process::Command::new(&executable);
            child
                .arg("--exact")
                .arg("runnable::tests::managed_admission_environment_child")
                .arg("--nocapture")
                .env("LOXA_MANAGED_ADMISSION_CHILD", "1")
                .env("LOXA_HOME", root.path())
                .env("PATH", path);
            match environment {
                Some(environment) => {
                    child.env("LOXA_LLAMA_SERVER", environment);
                }
                None => {
                    child.env_remove("LOXA_LLAMA_SERVER");
                }
            }
            let output = child.output().unwrap();
            assert!(
                output.status.success(),
                "{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn managed_admission_environment_child() {
        if std::env::var_os("LOXA_MANAGED_ADMISSION_CHILD").is_none() {
            return;
        }
        let paths = AppPaths::from_env().unwrap();
        let manifest = catalog::load_catalog(&paths.models).unwrap().remove(0);

        let runnable = resolve_managed_runnable(manifest, &paths).unwrap();

        assert_eq!(runnable.launch().server, paths.managed_server);
    }

    #[cfg(unix)]
    #[test]
    fn managed_admission_receipt_hit_keeps_fingerprint_and_model_lock() {
        use std::os::unix::fs::MetadataExt;

        let root = tempdir().unwrap();
        let paths = AppPaths::from_values(Some(root.path()), None).unwrap();
        let manifest = install_manifest(&paths);
        install_managed_server(&paths);
        let model_dir = paths.model_dir(&manifest.id).unwrap();
        let receipt = model_dir.join("verification-receipt.json");

        let first = resolve_managed_runnable(manifest.clone(), &paths).unwrap();
        let fingerprint = first.fingerprint().clone();
        assert!(
            ModelLock::acquire(&model_dir).is_err(),
            "Runnable must retain the model lock"
        );
        let receipt_inode = std::fs::metadata(&receipt).unwrap().ino();
        drop(first);
        drop(ModelLock::acquire(&model_dir).unwrap());

        let second = match resolve_managed_runnable_for_host(manifest, &paths, &|| true) {
            Ok(runnable) => runnable,
            Err(_) => panic!("an unchanged receipt hit consulted cancellation during hashing"),
        };

        assert_eq!(second.fingerprint(), &fingerprint);
        assert_eq!(
            std::fs::metadata(&receipt).unwrap().ino(),
            receipt_inode,
            "an unchanged receipt hit must not rewrite or rehash the artifact"
        );
        assert!(ModelLock::acquire(&model_dir).is_err());
        drop(second);
        drop(ModelLock::acquire(&model_dir).unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn managed_admission_cancellation_during_hash_is_typed_and_publishes_no_receipt() {
        for (cancel_on_poll, label) in [(2, "mid-hash"), (5, "before receipt publication")] {
            let root = tempdir().unwrap();
            let paths = AppPaths::from_values(Some(root.path()), None).unwrap();
            let mut bytes = vec![0x5a; 3 * 64 * 1024];
            bytes[..8].copy_from_slice(b"GGUF\x03\0\0\0");
            let mut digest = String::with_capacity(64);
            for byte in Sha256::digest(&bytes) {
                write!(&mut digest, "{byte:02x}").unwrap();
            }
            let mut manifest = manifest();
            manifest.size = bytes.len() as u64;
            manifest.sha256 = digest;
            let model_dir = paths.model_dir(&manifest.id).unwrap();
            std::fs::create_dir_all(&model_dir).unwrap();
            std::fs::write(model_dir.join("model.gguf"), bytes).unwrap();
            catalog::publish_manifest(&paths.models, &manifest).unwrap();
            drop(ModelLock::acquire(&model_dir).unwrap());
            install_managed_server(&paths);
            let receipt = model_dir.join("verification-receipt.json");
            std::fs::write(&receipt, b"stale").unwrap();
            let polls = Cell::new(0_usize);
            let receipt_was_published = Cell::new(false);

            let result = resolve_managed_runnable_for_host(manifest, &paths, &|| {
                receipt_was_published.set(receipt_was_published.get() || receipt.exists());
                let next = polls.get() + 1;
                polls.set(next);
                next == cancel_on_poll
            });

            assert!(
                matches!(result, Err(ManagedRunnableError::Cancelled)),
                "{label}"
            );
            assert_eq!(polls.get(), cancel_on_poll, "{label}");
            assert!(!receipt_was_published.get(), "{label}");
            assert!(!receipt.exists(), "{label}");
            assert!(!paths.run.join("foreground.json").exists(), "{label}");
            drop(ModelLock::acquire(&model_dir).unwrap());
        }
    }

    #[cfg(unix)]
    #[test]
    fn managed_admission_refuses_a_loose_local_candidate_without_adopting_it() {
        let root = tempdir().unwrap();
        let paths = AppPaths::from_values(Some(root.path()), None).unwrap();
        std::fs::create_dir_all(&paths.models).unwrap();
        let loose = paths.models.join("Demo.gguf");
        std::fs::write(&loose, b"GGUF\x03\0\0\0payload").unwrap();
        install_managed_server(&paths);

        let error = match resolve_managed_runnable(manifest(), &paths) {
            Err(error) => error,
            Ok(_) => panic!("loose local candidate was admitted as an installed model"),
        };

        assert!(error.contains("not installed"), "{error}");
        assert!(loose.is_file());
        assert!(!paths.models.join("demo/manifest.json").exists());
    }

    #[test]
    fn fingerprint_is_exact_and_changes_with_artifact_profile_or_context() {
        let manifest = manifest();
        let fingerprint =
            RuntimeFingerprint::from_manifest(&manifest, 4096, EffectiveProfile::Generic, Some(60))
                .unwrap();
        assert_eq!(
            serde_json::to_value(&fingerprint).unwrap(),
            serde_json::json!({
                "schema_version": 1,
                "model_id": "demo",
                "effective_context": 4096,
                "effective_profile": "generic",
                "sleep_policy": 60,
                "primary": {
                    "local_filename": "model.gguf",
                    "sha256": "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                    "size": 3
                },
                "draft": null
            })
        );

        let mut changed_artifact = manifest.clone();
        changed_artifact.sha256 = "f".repeat(64);
        assert_ne!(
            fingerprint,
            RuntimeFingerprint::from_manifest(
                &changed_artifact,
                4096,
                EffectiveProfile::Generic,
                Some(60),
            )
            .unwrap()
        );
        assert_ne!(
            fingerprint,
            RuntimeFingerprint::from_manifest(
                &manifest,
                8192,
                EffectiveProfile::Generic,
                Some(60),
            )
            .unwrap()
        );
        let mut changed_profile = serde_json::to_value(&fingerprint).unwrap();
        changed_profile["effective_profile"] = serde_json::json!("primary_only");
        assert_ne!(
            fingerprint,
            serde_json::from_value::<RuntimeFingerprint>(changed_profile).unwrap()
        );
    }

    #[test]
    fn mtp_primary_only_fingerprint_drops_draft_and_keeps_persistent_sleep_policy() {
        let fingerprint = RuntimeFingerprint::from_manifest(
            &bundle_manifest(),
            8192,
            EffectiveProfile::Gemma4Mtp,
            Some(60),
        )
        .unwrap();

        let primary_only = fingerprint.primary_only().unwrap();

        assert_eq!(
            primary_only.effective_profile(),
            EffectiveProfile::PrimaryOnly
        );
        assert!(primary_only.draft().is_none());
        assert_eq!(primary_only.sleep_policy(), Some(60));
    }

    #[test]
    fn expected_persistent_fingerprints_use_managed_config_without_locking_or_reading_artifacts() {
        let root = tempdir().unwrap();
        let paths = AppPaths::from_values(Some(root.path()), None).unwrap();
        let mut manifest = bundle_manifest();
        manifest.profile = Some(crate::catalog::TEST_MTP_PROFILE.into());
        manifest.runtime = Some(crate::catalog::RuntimeQualification {
            engine: "llama.cpp".into(),
            build: crate::catalog::TEST_LLAMA_BUILD.into(),
        });
        let model_dir = paths.model_dir(&manifest.id).unwrap();
        std::fs::create_dir_all(&model_dir).unwrap();
        let _busy = ModelLock::acquire(&model_dir).unwrap();
        std::fs::write(&paths.config, br#"{"version":1,"ctx":8192}"#).unwrap();

        let candidates = expected_persistent_fingerprints(&manifest, &paths).unwrap();
        let candidates = candidates.as_slice();

        assert_eq!(candidates.len(), 2);
        assert_eq!(
            candidates[0].effective_profile(),
            EffectiveProfile::Gemma4Mtp
        );
        assert_eq!(
            candidates[1].effective_profile(),
            EffectiveProfile::PrimaryOnly
        );
        assert_eq!(candidates[0].effective_context(), 8192);
        assert_eq!(candidates[1].effective_context(), 8192);
        assert_eq!(candidates[0].sleep_policy(), Some(60));
        assert_eq!(candidates[1].sleep_policy(), Some(60));
        assert!(candidates[0].draft().is_some());
        assert!(candidates[1].draft().is_none());
        assert!(!model_dir.join("verification-receipt.json").exists());
        assert!(!model_dir.join("model.gguf").exists());
        assert!(!model_dir.join("draft.gguf").exists());
    }

    #[cfg(unix)]
    #[test]
    fn unchanged_receipt_skips_the_full_artifact_verifier() {
        let root = tempdir().unwrap();
        let model_dir = root.path().join("demo");
        std::fs::create_dir(&model_dir).unwrap();
        let primary = model_dir.join("model.gguf");
        std::fs::write(&primary, b"abc").unwrap();
        let manifest = manifest();
        let model_lock = ModelLock::acquire(&model_dir).unwrap();
        let calls = Cell::new(0);

        verify_or_refresh(&model_lock, &model_dir, &manifest, &primary, None, || {
            calls.set(calls.get() + 1);
            Ok(verified_artifacts(&primary, None))
        })
        .unwrap();
        verify_or_refresh(&model_lock, &model_dir, &manifest, &primary, None, || {
            calls.set(calls.get() + 1);
            Ok(verified_artifacts(&primary, None))
        })
        .unwrap();

        assert_eq!(calls.get(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn receipt_miss_matrix_reverifies_missing_corrupt_manifest_and_same_size_replacement() {
        enum Change {
            Missing,
            Corrupt,
            Manifest,
            SameSizeReplacement,
        }

        for change in [
            Change::Missing,
            Change::Corrupt,
            Change::Manifest,
            Change::SameSizeReplacement,
        ] {
            let root = tempdir().unwrap();
            let model_dir = root.path().join("demo");
            std::fs::create_dir(&model_dir).unwrap();
            let primary = model_dir.join("model.gguf");
            std::fs::write(&primary, b"abc").unwrap();
            let manifest = manifest();
            let model_lock = ModelLock::acquire(&model_dir).unwrap();
            let calls = Cell::new(0);

            verify_or_refresh(&model_lock, &model_dir, &manifest, &primary, None, || {
                calls.set(calls.get() + 1);
                Ok(verified_artifacts(&primary, None))
            })
            .unwrap();

            let mut candidate = manifest.clone();
            let (label, valid) = match change {
                Change::Missing => {
                    std::fs::remove_file(model_dir.join("verification-receipt.json")).unwrap();
                    ("missing receipt", true)
                }
                Change::Corrupt => {
                    std::fs::write(model_dir.join("verification-receipt.json"), b"{not-json")
                        .unwrap();
                    ("corrupt receipt", true)
                }
                Change::Manifest => {
                    candidate.sha256 = "f".repeat(64);
                    ("manifest mismatch", false)
                }
                Change::SameSizeReplacement => {
                    let replacement = model_dir.join("replacement.gguf");
                    std::fs::write(&replacement, b"xyz").unwrap();
                    std::fs::rename(replacement, &primary).unwrap();
                    ("same-size replacement", false)
                }
            };

            let result =
                verify_or_refresh(&model_lock, &model_dir, &candidate, &primary, None, || {
                    calls.set(calls.get() + 1);
                    Ok(verified_artifacts(&primary, None))
                });

            assert_eq!(result.is_ok(), valid, "{label}");
            assert_eq!(calls.get(), 2, "{label} must invoke the full verifier");
        }
    }

    #[cfg(unix)]
    #[test]
    fn receipt_rechecks_optional_draft_identity_before_hitting() {
        let root = tempdir().unwrap();
        let model_dir = root.path().join("demo");
        std::fs::create_dir(&model_dir).unwrap();
        let primary = model_dir.join("model.gguf");
        let draft = model_dir.join("draft.gguf");
        std::fs::write(&primary, b"abc").unwrap();
        std::fs::write(&draft, b"draft").unwrap();
        let manifest = bundle_manifest();
        let model_lock = ModelLock::acquire(&model_dir).unwrap();
        let calls = Cell::new(0);

        verify_or_refresh(
            &model_lock,
            &model_dir,
            &manifest,
            &primary,
            Some(&draft),
            || {
                calls.set(calls.get() + 1);
                Ok(verified_artifacts(&primary, Some(&draft)))
            },
        )
        .unwrap();
        verify_or_refresh(
            &model_lock,
            &model_dir,
            &manifest,
            &primary,
            Some(&draft),
            || {
                calls.set(calls.get() + 1);
                Ok(verified_artifacts(&primary, Some(&draft)))
            },
        )
        .unwrap();
        let replacement = model_dir.join("replacement-draft.gguf");
        std::fs::write(&replacement, b"other").unwrap();
        std::fs::rename(replacement, &draft).unwrap();
        let result = verify_or_refresh(
            &model_lock,
            &model_dir,
            &manifest,
            &primary,
            Some(&draft),
            || {
                calls.set(calls.get() + 1);
                Ok(verified_artifacts(&primary, Some(&draft)))
            },
        );

        assert!(result.is_err());
        assert_eq!(calls.get(), 2);
    }

    #[cfg(unix)]
    #[test]
    fn swapped_model_directory_is_fail_closed_before_a_receipt_can_hit() {
        let root = tempdir().unwrap();
        let model_dir = root.path().join("demo");
        std::fs::create_dir(&model_dir).unwrap();
        let primary = model_dir.join("model.gguf");
        std::fs::write(&primary, b"abc").unwrap();
        let manifest = manifest();
        let model_lock = ModelLock::acquire(&model_dir).unwrap();
        let calls = Cell::new(0);

        verify_or_refresh(&model_lock, &model_dir, &manifest, &primary, None, || {
            calls.set(calls.get() + 1);
            Ok(verified_artifacts(&primary, None))
        })
        .unwrap();

        std::fs::rename(&model_dir, root.path().join("replaced-demo")).unwrap();
        std::fs::create_dir(&model_dir).unwrap();
        std::fs::write(&primary, b"abc").unwrap();

        let error = verify_or_refresh(&model_lock, &model_dir, &manifest, &primary, None, || {
            calls.set(calls.get() + 1);
            Ok(verified_artifacts(&primary, None))
        })
        .unwrap_err();

        assert!(error.contains("demo"), "{error}");
        assert_eq!(
            calls.get(),
            1,
            "a swapped directory must not hit or hash by path"
        );
    }

    #[cfg(unix)]
    #[test]
    fn failed_full_verification_leaves_no_receipt_to_trust_later() {
        let root = tempdir().unwrap();
        let model_dir = root.path().join("demo");
        std::fs::create_dir(&model_dir).unwrap();
        let primary = model_dir.join("model.gguf");
        std::fs::write(&primary, b"abc").unwrap();
        let manifest = manifest();
        let model_lock = ModelLock::acquire(&model_dir).unwrap();
        let receipt = model_dir.join("verification-receipt.json");

        verify_or_refresh(&model_lock, &model_dir, &manifest, &primary, None, || {
            Ok(verified_artifacts(&primary, None))
        })
        .unwrap();
        std::fs::write(&primary, b"xyz").unwrap();

        let error = verify_or_refresh(&model_lock, &model_dir, &manifest, &primary, None, || {
            Err("full verification failed".into())
        })
        .unwrap_err();

        assert_eq!(error, "full verification failed");
        assert!(!receipt.exists());
    }

    #[cfg(unix)]
    #[test]
    fn replacement_after_full_verification_cannot_receive_a_receipt() {
        let root = tempdir().unwrap();
        let model_dir = root.path().join("demo");
        std::fs::create_dir(&model_dir).unwrap();
        let primary = model_dir.join("model.gguf");
        std::fs::write(&primary, b"abc").unwrap();
        let manifest = manifest();
        let model_lock = ModelLock::acquire(&model_dir).unwrap();
        let receipt = model_dir.join("verification-receipt.json");

        let result = verify_or_refresh(&model_lock, &model_dir, &manifest, &primary, None, || {
            let verified = verified_artifacts(&primary, None);
            let replacement = model_dir.join("replacement.gguf");
            std::fs::write(&replacement, b"xyz").unwrap();
            std::fs::rename(replacement, &primary).unwrap();
            Ok(verified)
        });

        assert!(
            result.is_err(),
            "an unverified replacement must not be blessed"
        );
        assert!(!receipt.exists());
    }

    #[cfg(unix)]
    #[test]
    fn receipt_refresh_replaces_a_symlink_without_following_it() {
        use std::os::unix::fs::symlink;

        let root = tempdir().unwrap();
        let model_dir = root.path().join("demo");
        std::fs::create_dir(&model_dir).unwrap();
        let primary = model_dir.join("model.gguf");
        std::fs::write(&primary, b"abc").unwrap();
        let manifest = manifest();
        let model_lock = ModelLock::acquire(&model_dir).unwrap();
        let receipt_path = model_dir.join("verification-receipt.json");
        let outside = root.path().join("outside-receipt");
        std::fs::write(&outside, b"outside witness").unwrap();
        symlink(&outside, &receipt_path).unwrap();
        let verified = verified_file(&primary);

        refresh_verified(&model_lock, &manifest, &primary, &verified, None).unwrap();

        assert_eq!(std::fs::read(&outside).unwrap(), b"outside witness");
        assert!(std::fs::symlink_metadata(receipt_path)
            .unwrap()
            .file_type()
            .is_file());
    }

    #[cfg(unix)]
    #[test]
    fn receipt_refresh_rejects_a_proof_for_a_different_digest() {
        let root = tempdir().unwrap();
        let model_dir = root.path().join("demo");
        std::fs::create_dir(&model_dir).unwrap();
        let primary = model_dir.join("model.gguf");
        std::fs::write(&primary, b"abc").unwrap();
        let mut candidate = manifest();
        candidate.sha256 = "f".repeat(64);
        let model_lock = ModelLock::acquire(&model_dir).unwrap();
        let verified = verified_file(&primary);

        assert!(refresh_verified(&model_lock, &candidate, &primary, &verified, None).is_err());
        assert!(!model_dir.join("verification-receipt.json").exists());
    }
}
