//! Frozen bundle inventory, path layout, and whole-closure validation.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::paths::AppPaths;
use crate::runtime_identity::RuntimeIdentity;

use super::native::{parse_mach_o, validate_mach_o, MINIMUM_MACOS};

const INVENTORY_SCHEMA: u32 = 1;
const BUILD: &str = "b10344";
const COMMIT: &str = "7a20b417f4526cae073bd997af5020cea3e7ccbe";
const VERSION_LINE: &str = "version: 10344 (7a20b417f)";
const ARCHITECTURE: &str = "arm64";
pub(super) const LICENSE_SHA256: &str =
    "94f29bbed6a22c35b992c5c6ebf0e7c92f13b836b90f36f461c9cf2f0f1d010d";
pub(super) const PROVENANCE_SHA256: &str =
    "402029fbca52d7835acea49f515e83c3213e234bfbc78a474f7a0435afc95721";
const RELOCATION: &str = "install_name_tool -add_rpath @executable_path/../Frameworks llama-server";

pub(super) const MACH_O_FILES: &[&str] = &[
    "MacOS/llama-server",
    "Frameworks/libllama-server-impl.dylib",
    "Frameworks/libllama-common.0.0.10344.dylib",
    "Frameworks/libmtmd.0.0.10344.dylib",
    "Frameworks/libllama.0.0.10344.dylib",
    "Frameworks/libggml.0.19.0.dylib",
    "Frameworks/libggml-cpu.0.19.0.dylib",
    "Frameworks/libggml-blas.0.19.0.dylib",
    "Frameworks/libggml-metal.0.19.0.dylib",
    "Frameworks/libggml-rpc.0.19.0.dylib",
    "Frameworks/libggml-base.0.19.0.dylib",
];

const DATA_FILES: &[&str] = &[
    "Resources/loxa-runtime/b10344/LICENSE",
    "Resources/loxa-runtime/b10344/upstream-provenance.json",
    "Resources/loxa-runtime/b10344/normalized-inventory.json",
];

pub(super) const SYMLINKS: &[(&str, &str)] = &[
    (
        "Frameworks/libllama-common.0.dylib",
        "libllama-common.0.0.10344.dylib",
    ),
    ("Frameworks/libmtmd.0.dylib", "libmtmd.0.0.10344.dylib"),
    ("Frameworks/libllama.0.dylib", "libllama.0.0.10344.dylib"),
    ("Frameworks/libggml.0.dylib", "libggml.0.19.0.dylib"),
    ("Frameworks/libggml-cpu.0.dylib", "libggml-cpu.0.19.0.dylib"),
    (
        "Frameworks/libggml-blas.0.dylib",
        "libggml-blas.0.19.0.dylib",
    ),
    (
        "Frameworks/libggml-metal.0.dylib",
        "libggml-metal.0.19.0.dylib",
    ),
    ("Frameworks/libggml-rpc.0.dylib", "libggml-rpc.0.19.0.dylib"),
    (
        "Frameworks/libggml-base.0.dylib",
        "libggml-base.0.19.0.dylib",
    ),
];

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Inventory {
    schema_version: u32,
    runtime: InventoryRuntime,
    regular_files: Vec<InventoryRegular>,
    symlinks: Vec<InventorySymlink>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InventoryRuntime {
    build: String,
    commit: String,
    version_line: String,
    architecture: String,
    minimum_macos: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct InventoryRegular {
    path: String,
    pub(super) size: u64,
    pub(super) sha256: String,
    pub(super) mach_o: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InventorySymlink {
    path: String,
    target: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NormalizedInventory {
    schema_version: u32,
    relocation: String,
    regular_files: Vec<InventoryRegular>,
}

pub fn validate_embedded_runtime(paths: &AppPaths) -> Result<(), String> {
    let contents = embedded_contents(paths)?;
    let inventory_path = contents.join("Resources/loxa-runtime/b10344/inventory.json");

    let inventory_bytes = read_regular(&inventory_path, "inventory")?;
    if inventory_bytes.len() > 128 * 1024 {
        return Err("embedded runtime inventory is too large".into());
    }
    let inventory: Inventory = serde_json::from_slice(&inventory_bytes)
        .map_err(|_| "embedded runtime inventory is invalid".to_string())?;
    validate_inventory_identity(&inventory)?;
    let regular = validate_inventory_entries(&inventory)?;
    let symlinks = validate_inventory_symlinks(&inventory)?;

    let framework_names = MACH_O_FILES[1..]
        .iter()
        .chain(SYMLINKS.iter().map(|(path, _)| path))
        .map(|path| {
            Path::new(path)
                .file_name()
                .expect("fixed framework filename")
                .to_string_lossy()
                .into_owned()
        })
        .collect::<BTreeSet<_>>();
    for (relative, entry) in regular {
        let path = contents.join(relative);
        let bytes = read_regular(&path, relative)?;
        if bytes.len() as u64 != entry.size {
            return Err(format!("embedded runtime size mismatch for {relative}"));
        }
        if digest(&bytes) != entry.sha256 {
            return Err(format!("embedded runtime SHA-256 mismatch for {relative}"));
        }
        let executable = fs::symlink_metadata(&path)
            .map_err(|_| format!("embedded runtime file is missing: {relative}"))?
            .permissions()
            .mode()
            & 0o111
            != 0;
        if entry.mach_o != MACH_O_FILES.contains(&relative) {
            return Err(format!(
                "embedded runtime file kind is invalid for {relative}"
            ));
        }
        if entry.mach_o {
            if !executable {
                return Err(format!(
                    "embedded runtime Mach-O is not executable: {relative}"
                ));
            }
            let info = parse_mach_o(&bytes, relative)?;
            validate_mach_o(relative, &info, &framework_names)?;
        } else if executable {
            return Err(format!(
                "embedded runtime data file is executable: {relative}"
            ));
        }
    }

    for (relative, expected_target) in symlinks {
        let path = contents.join(relative);
        let metadata = fs::symlink_metadata(&path)
            .map_err(|_| format!("embedded runtime symlink is missing: {relative}"))?;
        if !metadata.file_type().is_symlink() {
            return Err(format!("embedded runtime symlink is unsafe: {relative}"));
        }
        let target = fs::read_link(&path)
            .map_err(|_| format!("embedded runtime symlink is unreadable: {relative}"))?;
        if target != Path::new(expected_target)
            || target.is_absolute()
            || target.components().count() != 1
        {
            return Err(format!(
                "embedded runtime symlink target is unsafe: {relative}"
            ));
        }
    }

    validate_exact_directory(
        &contents.join("Frameworks"),
        MACH_O_FILES[1..]
            .iter()
            .map(|path| Path::new(path).file_name().expect("fixed filename"))
            .chain(
                SYMLINKS
                    .iter()
                    .map(|(path, _)| Path::new(path).file_name().expect("fixed filename")),
            ),
        "Frameworks",
    )?;
    validate_exact_directory(
        &contents.join("Resources/loxa-runtime/b10344"),
        [
            "LICENSE",
            "upstream-provenance.json",
            "normalized-inventory.json",
            "inventory.json",
        ]
        .iter()
        .map(Path::new),
        "runtime resources",
    )?;

    let license = contents.join("Resources/loxa-runtime/b10344/LICENSE");
    if digest(&read_regular(&license, "LICENSE")?) != LICENSE_SHA256 {
        return Err("embedded runtime LICENSE does not match upstream".into());
    }
    let provenance = contents.join("Resources/loxa-runtime/b10344/upstream-provenance.json");
    if digest(&read_regular(&provenance, "provenance")?) != PROVENANCE_SHA256 {
        return Err("embedded runtime provenance does not match the frozen release".into());
    }
    validate_normalized_inventory(contents)?;
    Ok(())
}

pub(super) fn embedded_contents(paths: &AppPaths) -> Result<&Path, String> {
    if paths.runtime_identity != RuntimeIdentity::BundledB10344 {
        return Err("embedded runtime validation requires bundled b10344 mode".into());
    }
    let inventory_path = paths
        .runtime_inventory
        .as_deref()
        .ok_or("bundled runtime inventory path is missing")?;
    let contents = paths
        .managed_server
        .parent()
        .and_then(Path::parent)
        .ok_or("bundled runtime path is not inside Contents/MacOS")?;
    let expected_inventory = contents.join("Resources/loxa-runtime/b10344/inventory.json");
    if inventory_path != expected_inventory
        || paths.managed_server != contents.join("MacOS/llama-server")
    {
        return Err("bundled runtime layout does not match the application executable".into());
    }
    Ok(contents)
}

pub(super) fn validate_inventory_identity(inventory: &Inventory) -> Result<(), String> {
    let runtime = &inventory.runtime;
    if inventory.schema_version != INVENTORY_SCHEMA
        || runtime.build != BUILD
        || runtime.commit != COMMIT
        || runtime.version_line != VERSION_LINE
        || runtime.architecture != ARCHITECTURE
        || runtime.minimum_macos != MINIMUM_MACOS
    {
        return Err("embedded runtime inventory has the wrong version or identity".into());
    }
    Ok(())
}

pub(super) fn validate_inventory_entries(
    inventory: &Inventory,
) -> Result<BTreeMap<&str, &InventoryRegular>, String> {
    let expected = MACH_O_FILES
        .iter()
        .chain(DATA_FILES)
        .copied()
        .collect::<BTreeSet<_>>();
    let mut entries = BTreeMap::new();
    for entry in &inventory.regular_files {
        validate_relative_path(&entry.path)?;
        validate_sha256(&entry.sha256)?;
        if entry.size == 0 || entries.insert(entry.path.as_str(), entry).is_some() {
            return Err("embedded runtime inventory contains an invalid or duplicate file".into());
        }
    }
    if entries.keys().copied().collect::<BTreeSet<_>>() != expected {
        return Err("embedded runtime inventory has a missing or unexpected regular file".into());
    }
    Ok(entries)
}

pub(super) fn validate_inventory_symlinks(
    inventory: &Inventory,
) -> Result<BTreeMap<&str, &str>, String> {
    let expected = SYMLINKS.iter().copied().collect::<BTreeMap<_, _>>();
    let mut entries = BTreeMap::new();
    for entry in &inventory.symlinks {
        validate_relative_path(&entry.path)?;
        if Path::new(&entry.target).components().count() != 1
            || entries
                .insert(entry.path.as_str(), entry.target.as_str())
                .is_some()
        {
            return Err("embedded runtime inventory contains an unsafe symlink".into());
        }
    }
    if entries != expected {
        return Err("embedded runtime inventory has a missing or unexpected symlink".into());
    }
    Ok(entries)
}

fn validate_normalized_inventory(contents: &Path) -> Result<(), String> {
    let path = contents.join("Resources/loxa-runtime/b10344/normalized-inventory.json");
    let bytes = read_regular(&path, "normalized inventory")?;
    validate_normalized_inventory_bytes(&bytes)
}

pub(super) fn validate_normalized_inventory_bytes(bytes: &[u8]) -> Result<(), String> {
    let inventory: NormalizedInventory = serde_json::from_slice(bytes)
        .map_err(|_| "embedded normalized inventory is invalid".to_string())?;
    if inventory.schema_version != INVENTORY_SCHEMA || inventory.relocation != RELOCATION {
        return Err("embedded normalized inventory has invalid packaging metadata".into());
    }
    let mut paths = BTreeSet::new();
    for entry in &inventory.regular_files {
        validate_relative_path(&entry.path)?;
        validate_sha256(&entry.sha256)?;
        if !entry.mach_o || entry.size == 0 || !paths.insert(entry.path.as_str()) {
            return Err("embedded normalized inventory has an invalid code entry".into());
        }
    }
    if paths != MACH_O_FILES.iter().copied().collect() {
        return Err("embedded normalized inventory has a missing or unexpected code file".into());
    }
    Ok(())
}

fn validate_exact_directory<I, S>(directory: &Path, expected: I, label: &str) -> Result<(), String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    let expected = expected
        .into_iter()
        .map(|entry| PathBuf::from(entry.as_ref()))
        .collect::<BTreeSet<PathBuf>>();
    let entries = fs::read_dir(directory)
        .map_err(|_| format!("embedded runtime {label} directory is missing"))?
        .map(|entry| {
            entry
                .map(|entry| PathBuf::from(entry.file_name()))
                .map_err(|_| format!("embedded runtime {label} directory is unreadable"))
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    if entries != expected {
        return Err(format!(
            "embedded runtime {label} directory contains a missing or unexpected entry"
        ));
    }
    Ok(())
}

pub(super) fn read_regular(path: &Path, label: &str) -> Result<Vec<u8>, String> {
    let metadata =
        fs::symlink_metadata(path).map_err(|_| format!("embedded runtime {label} is missing"))?;
    if !metadata.file_type().is_file() || metadata.nlink() != 1 {
        return Err(format!(
            "embedded runtime {label} is not a safe regular file"
        ));
    }
    crate::safe_file::read_regular_file(path)
        .map_err(|_| format!("embedded runtime {label} changed while reading"))
}

fn validate_relative_path(path: &str) -> Result<(), String> {
    let parsed = Path::new(path);
    if path.is_empty()
        || parsed.is_absolute()
        || parsed.components().any(|component| {
            matches!(
                component,
                std::path::Component::ParentDir | std::path::Component::CurDir
            )
        })
        || path.contains('\\')
    {
        return Err("embedded runtime inventory contains an unsafe path".into());
    }
    Ok(())
}

fn validate_sha256(value: &str) -> Result<(), String> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err("embedded runtime inventory contains an invalid SHA-256".into());
    }
    Ok(())
}

pub(super) fn digest(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("writing to a String cannot fail");
    }
    output
}
