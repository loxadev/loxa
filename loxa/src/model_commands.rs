use loxa_core::download;
use loxa_core::hardware::HardwareReport;
use loxa_core::registry::{self, ModelEntry, VerifiedModel, REGISTRY};
use loxa_core::runtime_profile::runtime_profile;
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
            .all(|artifact| dir.join(artifact.filename()).exists());
        if all_final {
            return ModelStatus::Downloaded;
        }
        let any_present = artifacts.iter().any(|artifact| {
            dir.join(artifact.filename()).exists()
                || dir.join(format!("{}.part", artifact.filename())).exists()
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
    let mut removed = Vec::new();
    let paths = if let Some(profile) = runtime_profile(entry.id) {
        profile
            .artifacts()
            .into_iter()
            .flat_map(|artifact| artifact_paths(artifact, dir))
            .collect()
    } else {
        let (final_path, part_path) = model_paths(entry, dir);
        vec![final_path, part_path]
    };
    for path in paths {
        if path.try_exists()? {
            fs::remove_file(&path)?;
            removed.push(path);
        }
    }
    Ok(removed)
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
            || registry::load_user_entries(&user_registry_dir())
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
        return match download::download(&entry, &download::model_dir()) {
            Ok(path) => {
                registry::save_user_entry(&user_registry_dir(), &entry)
                    .map_err(io::Error::other)?;
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

    let dir = download::model_dir();
    match download_registry_entry_with(entry, &dir, download::download) {
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

fn user_registry_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
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
    stdout: &mut W,
    stderr: &mut E,
) -> io::Result<ExitCode> {
    let dir = download::model_dir();
    let removed = if let Some(entry) = REGISTRY.iter().find(|entry| entry.id == id) {
        remove_model_files(entry, &dir)?
    } else {
        let Some(removed) = remove_user_entry(id, &user_registry_dir(), &dir)? else {
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
        fs::write(temp.path().join(target.filename()), b"target").unwrap();
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
