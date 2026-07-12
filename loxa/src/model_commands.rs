use loxa_core::download;
use loxa_core::hardware::HardwareReport;
use loxa_core::registry::{self, ModelEntry, REGISTRY};
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
    let (final_path, part_path) = model_paths(entry, dir);
    let mut removed = Vec::new();
    for path in [final_path, part_path] {
        if path.try_exists()? {
            fs::remove_file(&path)?;
            removed.push(path);
        }
    }
    Ok(removed)
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
    match download::download(entry, &dir) {
        Ok(path) => {
            writeln!(stdout, "{}", path.display())?;
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
    let rows = REGISTRY
        .iter()
        .map(|entry| {
            (
                entry,
                bytes_to_gb_string(entry.size_bytes),
                model_status(entry, &dir).to_string(),
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
    for entry in registry::load_user_entries(&user_registry_dir()).map_err(io::Error::other)? {
        writeln!(
            stdout,
            "{:<id_width$}  {:<params_width$}  {:<quant_width$}  {:>size_width$}  {:<license_width$}  {:<status_width$}",
            entry.id,
            entry.params,
            entry.quant,
            bytes_to_gb_string(entry.size_bytes),
            entry.license,
            if download::model_dir().join(&entry.filename).exists() {
                "downloaded"
            } else {
                "not downloaded"
            },
        )?;
    }
    Ok(ExitCode::SUCCESS)
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
