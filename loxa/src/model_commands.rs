use loxa_core::download;
use loxa_core::hardware::HardwareReport;
use loxa_core::registry::{self, ModelEntry, VerifiedModel, REGISTRY};
use loxa_core::runtime_profile::runtime_profile;
use std::ffi::OsStr;
use std::fmt;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

pub(crate) fn bytes_to_gb_string(bytes: u64) -> String {
    format!("{:.1}", bytes as f64 / 1024_f64.powi(3))
}

fn valid_ids() -> String {
    REGISTRY
        .iter()
        .map(|entry| entry.id)
        .collect::<Vec<_>>()
        .join(", ")
}

pub(crate) fn model_paths(entry: &ModelEntry, dir: &Path) -> (PathBuf, PathBuf) {
    (
        dir.join(entry.filename),
        dir.join(format!("{}.part", entry.filename)),
    )
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ModelStatus {
    Downloaded,
    Partial,
    NotDownloaded,
}

impl fmt::Display for ModelStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Downloaded => formatter.write_str("downloaded"),
            Self::Partial => formatter.write_str("partial"),
            Self::NotDownloaded => formatter.write_str("not downloaded"),
        }
    }
}

pub(crate) fn model_status(entry: &ModelEntry, dir: &Path) -> ModelStatus {
    if let Some(profile) = runtime_profile(entry.id) {
        let artifacts = profile.artifacts();
        let all_final = artifacts
            .iter()
            .all(|artifact| exact_regular_final(*artifact, dir));
        if all_final {
            return ModelStatus::Downloaded;
        }
        let any_present = artifacts.iter().any(|artifact| {
            fs::symlink_metadata(dir.join(artifact.filename())).is_ok()
                || fs::symlink_metadata(dir.join(format!("{}.part", artifact.filename()))).is_ok()
        });
        return if any_present {
            ModelStatus::Partial
        } else {
            ModelStatus::NotDownloaded
        };
    }
    let (final_path, part_path) = model_paths(entry, dir);
    if final_path.exists() {
        ModelStatus::Downloaded
    } else if part_path.exists() {
        ModelStatus::Partial
    } else {
        ModelStatus::NotDownloaded
    }
}

pub(crate) fn remove_model_files(entry: &ModelEntry, dir: &Path) -> io::Result<Vec<PathBuf>> {
    if let Some(profile) = runtime_profile(entry.id) {
        let paths = profile
            .artifacts()
            .into_iter()
            .flat_map(|artifact| artifact_paths(artifact, dir))
            .collect::<Vec<_>>();
        let removable = preflight_paired_removal(&paths)?;
        for path in &removable {
            fs::remove_file(path)?;
        }
        return Ok(removable);
    }

    let (final_path, part_path) = model_paths(entry, dir);
    let mut removed = Vec::new();
    let paths = [final_path, part_path];
    for path in paths {
        if path.try_exists()? {
            fs::remove_file(&path)?;
            removed.push(path);
        }
    }
    Ok(removed)
}

fn exact_regular_final(artifact: &dyn VerifiedModel, dir: &Path) -> bool {
    fs::symlink_metadata(dir.join(artifact.filename())).is_ok_and(|metadata| {
        metadata.file_type().is_file()
            && artifact_has_single_link(&metadata)
            && metadata.len() == artifact.size_bytes()
    })
}

#[cfg(unix)]
fn artifact_has_single_link(metadata: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    metadata.nlink() == 1
}

#[cfg(windows)]
fn artifact_has_single_link(metadata: &fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;
    metadata.number_of_links() == Some(1)
}

#[cfg(not(any(unix, windows)))]
fn artifact_has_single_link(_metadata: &fs::Metadata) -> bool {
    false
}

fn preflight_paired_removal(paths: &[PathBuf]) -> io::Result<Vec<PathBuf>> {
    let mut removable = Vec::new();
    for path in paths {
        match fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_file() || metadata.file_type().is_symlink() => {
                removable.push(path.clone());
            }
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("refusing to remove unsafe artifact path {}", path.display()),
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    Ok(removable)
}

fn artifact_paths(artifact: &dyn VerifiedModel, dir: &Path) -> [PathBuf; 2] {
    [
        dir.join(artifact.filename()),
        dir.join(format!("{}.part", artifact.filename())),
    ]
}

fn download_registry_entry_with<F>(
    entry: &ModelEntry,
    dir: &Path,
    mut download_artifact: F,
) -> Result<Vec<PathBuf>, download::DownloadError>
where
    F: FnMut(&dyn VerifiedModel, &Path) -> Result<PathBuf, download::DownloadError>,
{
    if let Some(profile) = runtime_profile(entry.id) {
        return profile
            .artifacts()
            .into_iter()
            .map(|artifact| download_artifact(artifact, dir))
            .collect();
    }
    Ok(vec![download_artifact(entry, dir)?])
}

pub(crate) fn pull_model<W: Write, E: Write>(
    id: &str,
    quant: Option<&str>,
    models_dir: &Path,
    registry_dir: &Path,
    stdout: &mut W,
    stderr: &mut E,
) -> io::Result<ExitCode> {
    if id.starts_with("hf://") || id.matches('/').count() == 1 {
        let reference = loxa_core::resolve::ModelReference::parse(id).map_err(io::Error::other)?;
        let available = HardwareReport::detect().ram_available_bytes;
        let resolved = match loxa_core::resolve::resolve(&reference, quant, available) {
            Ok(resolved) => resolved,
            Err(error) => {
                writeln!(stderr, "pull resolution failed: {error}")?;
                return Ok(ExitCode::from(1));
            }
        };
        let generated_id = format!(
            "{}-{}",
            reference
                .repo
                .split('/')
                .next_back()
                .unwrap_or("model")
                .to_ascii_lowercase()
                .replace(|c: char| !c.is_ascii_alphanumeric(), "-"),
            resolved.quant.to_ascii_lowercase().replace('_', "-")
        );
        let entry = registry::UserModelEntry {
            id: generated_id,
            repo: resolved.repo,
            revision: resolved.revision,
            filename: resolved.filename,
            sha256: resolved.sha256,
            size_bytes: resolved.size_bytes,
            license: resolved.license,
            params: "unknown".into(),
            quant: resolved.quant,
            min_free_mem_gb: resolved.min_free_mem_gb,
        };
        if registry::find(&entry.id).is_some()
            || registry::load_user_entries(registry_dir)
                .map_err(io::Error::other)?
                .iter()
                .any(|old| old.id == entry.id)
        {
            writeln!(
                stderr,
                "model id {} already exists; run `loxa rm {}` first",
                entry.id, entry.id
            )?;
            return Ok(ExitCode::from(1));
        }
        writeln!(
            stdout,
            "selected {} ({}, {:.1} GB minimum free RAM)",
            entry.filename, entry.quant, entry.min_free_mem_gb
        )?;
        return match download::download(&entry, models_dir) {
            Ok(path) => {
                registry::save_user_entry(registry_dir, &entry).map_err(io::Error::other)?;
                writeln!(stdout, "{}", path.display())?;
                Ok(ExitCode::SUCCESS)
            }
            Err(error) => {
                writeln!(stderr, "pull failed for {}: {error}", entry.id)?;
                Ok(ExitCode::from(1))
            }
        };
    }
    let Some(entry) = registry::find(id) else {
        write_unknown_id(id, stderr)?;
        return Ok(ExitCode::from(1));
    };

    match download_registry_entry_with(entry, models_dir, download::download) {
        Ok(paths) => {
            for path in paths {
                writeln!(stdout, "{}", path.display())?;
            }
            Ok(ExitCode::SUCCESS)
        }
        Err(error) => {
            writeln!(stderr, "pull failed for {id}: {error}")?;
            Ok(ExitCode::from(1))
        }
    }
}

pub(crate) fn user_registry_dir() -> PathBuf {
    let home = std::env::var_os("HOME");
    user_registry_dir_from_home(home.as_deref())
}

pub(crate) fn user_registry_dir_from_home(home: Option<&OsStr>) -> PathBuf {
    home.map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".loxa/registry.d")
}

pub(crate) fn print_list<W: Write>(stdout: &mut W) -> io::Result<ExitCode> {
    let dir = download::model_dir();
    let user_entries =
        registry::load_user_entries(&user_registry_dir()).map_err(io::Error::other)?;
    write_model_list(stdout, &dir, &user_entries)?;
    Ok(ExitCode::SUCCESS)
}

#[cfg(test)]
fn write_compiled_list<W: Write>(stdout: &mut W, dir: &Path) -> io::Result<()> {
    write_model_list(stdout, dir, &[])
}

fn write_model_list<W: Write>(
    stdout: &mut W,
    dir: &Path,
    user_entries: &[registry::UserModelEntry],
) -> io::Result<()> {
    let rows = REGISTRY
        .iter()
        .map(|entry| {
            (
                entry,
                bytes_to_gb_string(
                    runtime_profile(entry.id)
                        .map(|profile| profile.total_size_bytes())
                        .unwrap_or(entry.size_bytes),
                ),
                model_status(entry, dir).to_string(),
            )
        })
        .collect::<Vec<_>>();

    let id_width = rows
        .iter()
        .map(|(entry, _, _)| entry.id.len())
        .chain([2])
        .max()
        .unwrap_or(2);
    let params_width = rows
        .iter()
        .map(|(entry, _, _)| entry.params.len())
        .chain([6])
        .max()
        .unwrap_or(6);
    let quant_width = rows
        .iter()
        .map(|(entry, _, _)| entry.quant.len())
        .chain([5])
        .max()
        .unwrap_or(5);
    let size_width = rows
        .iter()
        .map(|(_, size, _)| size.len())
        .chain([7])
        .max()
        .unwrap_or(7);
    let license_width = rows
        .iter()
        .map(|(entry, _, _)| entry.license.len())
        .chain([7])
        .max()
        .unwrap_or(7);
    let status_width = rows
        .iter()
        .map(|(_, _, status)| status.len())
        .chain([6])
        .max()
        .unwrap_or(6);

    writeln!(
        stdout,
        "{:<id_width$}  {:<params_width$}  {:<quant_width$}  {:>size_width$}  {:<license_width$}  {:<status_width$}",
        "id", "params", "quant", "size GB", "license", "status",
    )?;
    for (entry, size, status) in rows {
        writeln!(
            stdout,
            "{:<id_width$}  {:<params_width$}  {:<quant_width$}  {:>size_width$}  {:<license_width$}  {:<status_width$}",
            entry.id, entry.params, entry.quant, size, entry.license, status,
        )?;
    }
    for entry in user_entries {
        writeln!(
            stdout,
            "{:<id_width$}  {:<params_width$}  {:<quant_width$}  {:>size_width$}  {:<license_width$}  {:<status_width$}",
            entry.id,
            entry.params,
            entry.quant,
            bytes_to_gb_string(entry.size_bytes),
            entry.license,
            if dir.join(&entry.filename).exists() {
                "downloaded"
            } else {
                "not downloaded"
            },
        )?;
    }
    Ok(())
}

pub(crate) fn remove_model<W: Write, E: Write>(
    id: &str,
    models_dir: &Path,
    registry_dir: &Path,
    stdout: &mut W,
    stderr: &mut E,
) -> io::Result<ExitCode> {
    let removed = if let Some(entry) = REGISTRY.iter().find(|entry| entry.id == id) {
        remove_model_files(entry, models_dir)?
    } else {
        let Some(removed) = remove_user_entry(id, registry_dir, models_dir)? else {
            write_unknown_id(id, stderr)?;
            return Ok(ExitCode::from(1));
        };
        removed
    };
    if removed.is_empty() {
        writeln!(stdout, "nothing present for {id}")?;
    } else {
        for path in removed {
            writeln!(stdout, "removed {}", path.display())?;
        }
    }
    Ok(ExitCode::SUCCESS)
}

pub(crate) fn remove_user_entry(
    id: &str,
    registry_dir: &Path,
    models_dir: &Path,
) -> io::Result<Option<Vec<PathBuf>>> {
    let entries = registry::load_user_entries(registry_dir).map_err(io::Error::other)?;
    let Some(entry) = entries.into_iter().find(|entry| entry.id == id) else {
        return Ok(None);
    };
    let mut removed = Vec::new();
    for path in [
        models_dir.join(&entry.filename),
        models_dir.join(format!("{}.part", entry.filename)),
        registry_dir.join(format!("{}.json", entry.id)),
    ] {
        if path.try_exists()? {
            fs::remove_file(&path)?;
            removed.push(path);
        }
    }
    Ok(Some(removed))
}

pub(crate) fn write_unknown_id<W: Write>(id: &str, stderr: &mut W) -> io::Result<()> {
    writeln!(stderr, "unknown model id: {id}")?;
    writeln!(stderr, "valid ids: {}", valid_ids())
}

#[cfg(test)]
mod tests {
    use super::*;
    use loxa_core::download::DownloadError;
    use loxa_core::registry::VerifiedModel;
    use loxa_core::runtime_profile::runtime_profile;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn user_registry_resolver_uses_only_home_and_preserves_the_no_home_fallback() {
        assert_eq!(
            user_registry_dir_from_home(Some(std::ffi::OsStr::new("home-root"))),
            PathBuf::from("home-root").join(".loxa/registry.d")
        );
        assert_eq!(
            user_registry_dir_from_home(None),
            PathBuf::from(".").join(".loxa/registry.d")
        );
    }

    #[test]
    fn fixed_pair_status_and_list_use_aggregate_artifacts_and_size() {
        let temp = TempDir::new("loxa-pair-list");
        let entry = registry::find("loxa").expect("loxa registry entry");
        let profile = runtime_profile("loxa").expect("loxa runtime profile");
        let [target, drafter] = profile.artifacts();

        assert_eq!(model_status(entry, temp.path()), ModelStatus::NotDownloaded);

        fs::write(temp.path().join(target.filename()), b"target").unwrap();
        assert_eq!(model_status(entry, temp.path()), ModelStatus::Partial);

        fs::remove_file(temp.path().join(target.filename())).unwrap();
        fs::write(temp.path().join(drafter.filename()), b"drafter").unwrap();
        assert_eq!(model_status(entry, temp.path()), ModelStatus::Partial);

        fs::write(
            temp.path().join(format!("{}.part", target.filename())),
            b"partial",
        )
        .unwrap();
        assert_eq!(model_status(entry, temp.path()), ModelStatus::Partial);

        let mut output = Vec::new();
        write_compiled_list(&mut output, temp.path()).unwrap();
        let output = String::from_utf8(output).unwrap();
        let loxa_row = output
            .lines()
            .find(|line| line.starts_with("loxa "))
            .expect("loxa list row");
        assert!(loxa_row.contains("6.5"));
        assert!(loxa_row.contains("partial"));

        fs::remove_file(temp.path().join(format!("{}.part", target.filename()))).unwrap();
        write_sparse(&temp.path().join(target.filename()), target.size_bytes());
        write_sparse(&temp.path().join(drafter.filename()), drafter.size_bytes());
        assert_eq!(model_status(entry, temp.path()), ModelStatus::Downloaded);
    }

    #[test]
    fn fixed_pair_pull_downloads_target_then_drafter_and_stops_on_failure() {
        let temp = TempDir::new("loxa-pair-pull-failure");
        let entry = registry::find("loxa").expect("loxa registry entry");
        let mut calls = Vec::new();

        let error = download_registry_entry_with(entry, temp.path(), |artifact, dir| {
            calls.push(artifact.filename().to_string());
            if calls.len() == 2 {
                return Err(DownloadError::Http("drafter failed".into()));
            }
            let path = dir.join(artifact.filename());
            fs::write(&path, b"target").unwrap();
            Ok(path)
        })
        .unwrap_err();

        assert!(error.to_string().contains("drafter failed"));
        assert_eq!(
            calls,
            vec![
                "gemma-4-12B-it-qat-UD-Q4_K_XL.gguf",
                "mtp-gemma-4-12B-it.gguf",
            ]
        );
        assert!(temp
            .path()
            .join("gemma-4-12B-it-qat-UD-Q4_K_XL.gguf")
            .exists());
        assert!(!temp.path().join("mtp-gemma-4-12B-it.gguf").exists());
    }

    #[test]
    fn fixed_pair_pull_succeeds_only_after_both_downloads() {
        let temp = TempDir::new("loxa-pair-pull-success");
        let entry = registry::find("loxa").expect("loxa registry entry");
        let mut calls = Vec::new();

        let paths = download_registry_entry_with(entry, temp.path(), |artifact, dir| {
            calls.push(artifact.filename().to_string());
            let path = dir.join(artifact.filename());
            fs::write(&path, artifact.filename().as_bytes()).unwrap();
            Ok(path)
        })
        .unwrap();

        assert_eq!(calls.len(), 2);
        assert_eq!(paths.len(), 2);
        assert!(paths.iter().all(|path| path.exists()));
        assert_eq!(
            paths[1].file_name().unwrap(),
            std::ffi::OsStr::new("mtp-gemma-4-12B-it.gguf")
        );
    }

    #[test]
    fn fixed_pair_removal_deletes_both_final_and_partial_files() {
        let temp = TempDir::new("loxa-pair-remove");
        let entry = registry::find("loxa").expect("loxa registry entry");
        let profile = runtime_profile("loxa").expect("loxa runtime profile");
        let expected = profile
            .artifacts()
            .into_iter()
            .flat_map(|artifact| {
                [
                    temp.path().join(artifact.filename()),
                    temp.path().join(format!("{}.part", artifact.filename())),
                ]
            })
            .collect::<Vec<_>>();
        for path in &expected {
            fs::write(path, b"bytes").unwrap();
        }

        let removed = remove_model_files(entry, temp.path()).unwrap();

        assert_eq!(removed, expected);
        assert!(removed.iter().all(|path| !path.exists()));
    }

    #[test]
    fn fixed_pair_status_requires_both_exact_regular_final_sizes() {
        let temp = TempDir::new("loxa-pair-status-exact");
        let entry = registry::find("loxa").expect("loxa registry entry");
        let profile = runtime_profile("loxa").expect("loxa runtime profile");
        let [target, drafter] = profile.artifacts();
        write_sparse(&temp.path().join(target.filename()), target.size_bytes());
        write_sparse(&temp.path().join(drafter.filename()), drafter.size_bytes());
        assert_eq!(model_status(entry, temp.path()), ModelStatus::Downloaded);

        write_sparse(
            &temp.path().join(drafter.filename()),
            drafter.size_bytes() - 1,
        );
        assert_eq!(model_status(entry, temp.path()), ModelStatus::Partial);

        fs::remove_file(temp.path().join(drafter.filename())).unwrap();
        fs::create_dir(temp.path().join(drafter.filename())).unwrap();
        assert_eq!(model_status(entry, temp.path()), ModelStatus::Partial);
    }

    #[cfg(unix)]
    #[test]
    fn fixed_pair_status_rejects_symlink_and_hardlink_finals() {
        use std::os::unix::fs::symlink;

        let entry = registry::find("loxa").expect("loxa registry entry");
        let profile = runtime_profile("loxa").expect("loxa runtime profile");
        let [target, drafter] = profile.artifacts();

        let symlink_temp = TempDir::new("loxa-pair-status-symlink");
        write_sparse(
            &symlink_temp.path().join(target.filename()),
            target.size_bytes(),
        );
        let outside = symlink_temp.path().join("outside-drafter.gguf");
        write_sparse(&outside, drafter.size_bytes());
        symlink(&outside, symlink_temp.path().join(drafter.filename())).unwrap();
        assert_eq!(
            model_status(entry, symlink_temp.path()),
            ModelStatus::Partial
        );

        let hardlink_temp = TempDir::new("loxa-pair-status-hardlink");
        write_sparse(
            &hardlink_temp.path().join(target.filename()),
            target.size_bytes(),
        );
        let hardlink_source = hardlink_temp.path().join("drafter-source.gguf");
        write_sparse(&hardlink_source, drafter.size_bytes());
        fs::hard_link(
            &hardlink_source,
            hardlink_temp.path().join(drafter.filename()),
        )
        .unwrap();
        assert_eq!(
            model_status(entry, hardlink_temp.path()),
            ModelStatus::Partial
        );
    }

    #[cfg(unix)]
    #[test]
    fn fixed_pair_removal_deletes_a_dangling_symlink() {
        use std::os::unix::fs::symlink;

        let temp = TempDir::new("loxa-pair-remove-dangling");
        let entry = registry::find("loxa").expect("loxa registry entry");
        let profile = runtime_profile("loxa").expect("loxa runtime profile");
        let dangling = temp.path().join(profile.drafter.filename());
        symlink(temp.path().join("missing-target"), &dangling).unwrap();

        let removed = remove_model_files(entry, temp.path()).unwrap();

        assert_eq!(removed, vec![dangling.clone()]);
        assert!(fs::symlink_metadata(dangling).is_err());
    }

    #[test]
    fn fixed_pair_removal_rejects_unsafe_type_before_removing_any_file() {
        let temp = TempDir::new("loxa-pair-remove-preflight");
        let entry = registry::find("loxa").expect("loxa registry entry");
        let profile = runtime_profile("loxa").expect("loxa runtime profile");
        let target = temp.path().join(profile.target.filename());
        let unsafe_drafter = temp.path().join(profile.drafter.filename());
        fs::write(&target, b"target").unwrap();
        fs::create_dir(&unsafe_drafter).unwrap();

        let error = remove_model_files(entry, temp.path()).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(target.exists());
        assert!(unsafe_drafter.is_dir());
    }

    fn write_sparse(path: &Path, size: u64) {
        fs::File::create(path).unwrap().set_len(size).unwrap();
    }

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(prefix: &str) -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path =
                std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()));
            fs::create_dir_all(&path).unwrap();
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}
