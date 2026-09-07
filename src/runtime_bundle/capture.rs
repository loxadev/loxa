//! Descriptor-held source capture and the final source-identity fence.

use std::collections::BTreeSet;
use std::fs::File;
use std::io::Read as _;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use crate::paths::AppPaths;

use super::inventory::{
    digest, embedded_contents, validate_inventory_entries, validate_inventory_identity,
    validate_inventory_symlinks, validate_normalized_inventory_bytes, Inventory, LICENSE_SHA256,
    MACH_O_FILES, PROVENANCE_SHA256, SYMLINKS,
};
use super::native::{parse_mach_o, validate_mach_o};

pub(super) struct CapturedRegular {
    pub(super) relative: String,
    file: File,
    identity: crate::safe_file::RegularFileIdentity,
    source_path: PathBuf,
    pub(super) bytes: Vec<u8>,
    pub(super) mode: u32,
}

struct SourceDirectory {
    file: File,
    identity: crate::safe_file::DirectoryIdentity,
    path: PathBuf,
}

struct SourceCapabilities {
    contents: SourceDirectory,
    macos: SourceDirectory,
    frameworks: SourceDirectory,
    resources: SourceDirectory,
    runtime_parent: SourceDirectory,
    runtime: SourceDirectory,
}

pub(super) fn capture_source_closure(
    paths: &AppPaths,
    after_inventory_open: impl FnOnce(),
    after_capture_fence: impl FnOnce(),
) -> Result<Vec<CapturedRegular>, String> {
    let source = open_source_capabilities(paths)?;
    let mut captured = vec![capture_regular_at(
        &source.runtime,
        std::ffi::OsStr::new("inventory.json"),
        "Resources/loxa-runtime/b10344/inventory.json",
        after_inventory_open,
    )?];
    let inventory_bytes = &captured[0].bytes;
    if inventory_bytes.len() > 128 * 1024 {
        return Err("embedded runtime inventory is too large".into());
    }
    let inventory: Inventory = serde_json::from_slice(inventory_bytes)
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
        let (directory, name) = source_file_location(&source, relative)?;
        let file = capture_regular_at(directory, name, relative, || {})?;
        if file.bytes.len() as u64 != entry.size {
            return Err(format!("embedded runtime size mismatch for {relative}"));
        }
        if digest(&file.bytes) != entry.sha256 {
            return Err(format!("embedded runtime SHA-256 mismatch for {relative}"));
        }
        let executable = file.mode & 0o111 != 0;
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
            let info = parse_mach_o(&file.bytes, relative)?;
            validate_mach_o(relative, &info, &framework_names)?;
        } else if executable {
            return Err(format!(
                "embedded runtime data file is executable: {relative}"
            ));
        }
        captured.push(file);
    }

    for (relative, expected_target) in symlinks {
        let name = Path::new(relative)
            .file_name()
            .expect("validated framework symlink path");
        let target = rustix::fs::readlinkat(&source.frameworks.file, name, Vec::new())
            .map_err(|_| format!("embedded runtime symlink is unreadable: {relative}"))?;
        if target.as_bytes() != expected_target.as_bytes() {
            return Err(format!(
                "embedded runtime symlink target is unsafe: {relative}"
            ));
        }
    }

    validate_exact_directory_descriptor(
        &source.frameworks.file,
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
    validate_exact_directory_descriptor(
        &source.runtime.file,
        [
            "LICENSE",
            "upstream-provenance.json",
            "normalized-inventory.json",
            "inventory.json",
        ],
        "runtime resources",
    )?;

    let captured_bytes = |relative: &str| {
        captured
            .iter()
            .find(|file| file.relative == relative)
            .map(|file| file.bytes.as_slice())
            .expect("inventory requires every captured regular")
    };
    if digest(captured_bytes("Resources/loxa-runtime/b10344/LICENSE")) != LICENSE_SHA256 {
        return Err("embedded runtime LICENSE does not match upstream".into());
    }
    if digest(captured_bytes(
        "Resources/loxa-runtime/b10344/upstream-provenance.json",
    )) != PROVENANCE_SHA256
    {
        return Err("embedded runtime provenance does not match the frozen release".into());
    }
    validate_normalized_inventory_bytes(captured_bytes(
        "Resources/loxa-runtime/b10344/normalized-inventory.json",
    ))?;
    fence_source_capabilities(&source, &captured)?;
    after_capture_fence();
    Ok(captured)
}

fn open_source_capabilities(paths: &AppPaths) -> Result<SourceCapabilities, String> {
    let contents_path = embedded_contents(paths)?.to_path_buf();
    let (contents_file, contents_identity) = crate::safe_file::open_directory(&contents_path)
        .map_err(|_| "embedded runtime Contents directory is unsafe".to_string())?;
    let contents = SourceDirectory {
        file: contents_file,
        identity: contents_identity,
        path: contents_path,
    };
    let macos = open_directory_at(&contents, "MacOS", "MacOS")?;
    let frameworks = open_directory_at(&contents, "Frameworks", "Frameworks")?;
    let resources = open_directory_at(&contents, "Resources", "Resources")?;
    let runtime_parent = open_directory_at(&resources, "loxa-runtime", "runtime resources")?;
    let runtime = open_directory_at(&runtime_parent, "b10344", "runtime resources")?;
    Ok(SourceCapabilities {
        contents,
        macos,
        frameworks,
        resources,
        runtime_parent,
        runtime,
    })
}

fn open_directory_at(
    parent: &SourceDirectory,
    name: &str,
    label: &str,
) -> Result<SourceDirectory, String> {
    let descriptor = rustix::fs::openat(
        &parent.file,
        name,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::DIRECTORY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map_err(|_| format!("embedded runtime {label} directory is unsafe"))?;
    let file = File::from(descriptor);
    let path = parent.path.join(name);
    let identity = crate::safe_file::directory_identity(&file, &path)
        .map_err(|_| format!("embedded runtime {label} directory is unsafe"))?;
    Ok(SourceDirectory {
        file,
        identity,
        path,
    })
}

fn source_file_location<'a>(
    source: &'a SourceCapabilities,
    relative: &'a str,
) -> Result<(&'a SourceDirectory, &'a std::ffi::OsStr), String> {
    let path = Path::new(relative);
    let name = path
        .file_name()
        .ok_or_else(|| "embedded runtime inventory contains an unsafe path".to_string())?;
    if path.parent() == Some(Path::new("MacOS")) {
        Ok((&source.macos, name))
    } else if path.parent() == Some(Path::new("Frameworks")) {
        Ok((&source.frameworks, name))
    } else if path.parent() == Some(Path::new("Resources/loxa-runtime/b10344")) {
        Ok((&source.runtime, name))
    } else {
        Err("embedded runtime inventory contains an unsafe path".into())
    }
}

fn capture_regular_at(
    directory: &SourceDirectory,
    name: &std::ffi::OsStr,
    relative: &str,
    after_open: impl FnOnce(),
) -> Result<CapturedRegular, String> {
    let descriptor = rustix::fs::openat(
        &directory.file,
        name,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::NONBLOCK
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map_err(|_| format!("embedded runtime file is unsafe: {relative}"))?;
    let mut file = File::from(descriptor);
    let source_path = directory.path.join(name);
    let identity = crate::safe_file::regular_file_identity(&file, &source_path)
        .map_err(|_| format!("embedded runtime file is unsafe: {relative}"))?;
    let metadata = file
        .metadata()
        .map_err(|_| format!("embedded runtime file is unreadable: {relative}"))?;
    after_open();
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|_| format!("embedded runtime file changed while reading: {relative}"))?;
    if crate::safe_file::regular_file_identity(&file, &source_path)
        .map_err(|_| format!("embedded runtime file changed while reading: {relative}"))?
        != identity
    {
        return Err(format!(
            "embedded runtime file changed while reading: {relative}"
        ));
    }
    Ok(CapturedRegular {
        relative: relative.to_owned(),
        file,
        identity,
        source_path,
        bytes,
        mode: metadata.permissions().mode() & 0o777,
    })
}

fn validate_exact_directory_descriptor<I, S>(
    directory: &File,
    expected: I,
    label: &str,
) -> Result<(), String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    use std::os::unix::ffi::OsStringExt as _;

    let expected = expected
        .into_iter()
        .map(|entry| PathBuf::from(entry.as_ref()))
        .collect::<BTreeSet<_>>();
    let mut stream = rustix::fs::Dir::read_from(directory)
        .map_err(|_| format!("embedded runtime {label} directory is unreadable"))?;
    let mut entries = BTreeSet::new();
    while let Some(entry) = stream.read() {
        let entry =
            entry.map_err(|_| format!("embedded runtime {label} directory is unreadable"))?;
        let name = entry.file_name().to_bytes();
        if name != b"." && name != b".." {
            entries.insert(PathBuf::from(std::ffi::OsString::from_vec(name.to_vec())));
        }
    }
    if entries != expected {
        return Err(format!(
            "embedded runtime {label} directory contains a missing or unexpected entry"
        ));
    }
    Ok(())
}

fn fence_source_capabilities(
    source: &SourceCapabilities,
    captured: &[CapturedRegular],
) -> Result<(), String> {
    for directory in [
        &source.contents,
        &source.macos,
        &source.frameworks,
        &source.resources,
        &source.runtime_parent,
        &source.runtime,
    ] {
        crate::safe_file::ensure_directory_descriptor_matches_path(
            &directory.file,
            &directory.identity,
            &directory.path,
        )
        .map_err(|_| "embedded runtime source changed while capturing".to_string())?;
    }
    for regular in captured {
        crate::safe_file::ensure_descriptor_matches_path(
            &regular.file,
            &regular.identity,
            &regular.source_path,
        )
        .map_err(|_| "embedded runtime source changed while capturing".to_string())?;
    }
    Ok(())
}
