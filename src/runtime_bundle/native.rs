//! Thin arm64 Mach-O parsing and loader dependency validation.

use std::collections::BTreeSet;

pub(super) const MINIMUM_MACOS: &str = "13.3";
const MAXIMUM_MINOS: u32 = (13 << 16) | (3 << 8);

pub(super) struct MachOInfo {
    minimum_macos: u32,
    rpaths: Vec<String>,
    dependencies: Vec<String>,
    install_name: Option<String>,
}

pub(super) fn parse_mach_o(bytes: &[u8], relative: &str) -> Result<MachOInfo, String> {
    if bytes.len() < 32
        || u32::from_le_bytes(bytes[0..4].try_into().expect("four-byte slice")) != 0xfeedfacf
        || u32::from_le_bytes(bytes[4..8].try_into().expect("four-byte slice")) != 0x0100000c
    {
        return Err(format!(
            "embedded runtime Mach-O must be thin arm64: {relative}"
        ));
    }
    let command_count = read_u32(bytes, 16, relative)? as usize;
    let command_bytes = read_u32(bytes, 20, relative)? as usize;
    let command_end = 32_usize
        .checked_add(command_bytes)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| format!("embedded runtime Mach-O load commands are invalid: {relative}"))?;
    let mut offset = 32_usize;
    let mut minimum_macos = None;
    let mut rpaths = Vec::new();
    let mut dependencies = Vec::new();
    let mut install_name = None;
    for _ in 0..command_count {
        let kind = read_u32(bytes, offset, relative)?;
        let size = read_u32(bytes, offset + 4, relative)? as usize;
        let next = offset
            .checked_add(size)
            .filter(|next| size >= 8 && *next <= command_end)
            .ok_or_else(|| {
                format!("embedded runtime Mach-O load command is invalid: {relative}")
            })?;
        match kind {
            0x32 => {
                if read_u32(bytes, offset + 8, relative)? != 1 || minimum_macos.is_some() {
                    return Err(format!(
                        "embedded runtime Mach-O platform metadata is invalid: {relative}"
                    ));
                }
                minimum_macos = Some(read_u32(bytes, offset + 12, relative)?);
            }
            0x8000001c => rpaths.push(read_load_string(bytes, offset, next, relative)?),
            0x0c | 0x80000018 | 0x8000001f | 0x80000023 => {
                dependencies.push(read_load_string(bytes, offset, next, relative)?)
            }
            0x0d => install_name = Some(read_load_string(bytes, offset, next, relative)?),
            _ => {}
        }
        offset = next;
    }
    if offset != command_end {
        return Err(format!(
            "embedded runtime Mach-O load command size is invalid: {relative}"
        ));
    }
    Ok(MachOInfo {
        minimum_macos: minimum_macos.ok_or_else(|| {
            format!("embedded runtime Mach-O is missing LC_BUILD_VERSION: {relative}")
        })?,
        rpaths,
        dependencies,
        install_name,
    })
}

pub(super) fn validate_mach_o(
    relative: &str,
    info: &MachOInfo,
    framework_names: &BTreeSet<String>,
) -> Result<(), String> {
    if info.minimum_macos > MAXIMUM_MINOS {
        return Err(format!(
            "embedded runtime minimum macOS exceeds {MINIMUM_MACOS}: {relative}"
        ));
    }
    if relative == "MacOS/llama-server" {
        if !info
            .rpaths
            .iter()
            .any(|path| path == "@executable_path/../Frameworks")
        {
            return Err("embedded llama-server is missing the Frameworks rpath".into());
        }
        if info.install_name.is_some() {
            return Err("embedded llama-server has an unexpected install name".into());
        }
    } else {
        if !info.rpaths.iter().any(|path| path == "@loader_path") {
            return Err(format!(
                "embedded runtime dylib has no loader rpath: {relative}"
            ));
        }
        let install_name = info
            .install_name
            .as_deref()
            .ok_or_else(|| format!("embedded runtime dylib has no install name: {relative}"))?;
        let Some(name) = install_name.strip_prefix("@rpath/") else {
            return Err(format!(
                "embedded runtime dylib install name is unsafe: {relative}"
            ));
        };
        if !framework_names.contains(name) {
            return Err(format!(
                "embedded runtime dylib install name is unresolved: {relative}"
            ));
        }
    }
    for dependency in &info.dependencies {
        if dependency.starts_with("/System/Library/") || dependency.starts_with("/usr/lib/") {
            continue;
        }
        let Some(name) = dependency.strip_prefix("@rpath/") else {
            return Err(format!("embedded runtime dependency is unsafe: {relative}"));
        };
        if !framework_names.contains(name) {
            return Err(format!(
                "embedded runtime dependency is unresolved: {relative}"
            ));
        }
    }
    Ok(())
}

fn read_load_string(
    bytes: &[u8],
    command: usize,
    command_end: usize,
    relative: &str,
) -> Result<String, String> {
    let string_offset = read_u32(bytes, command + 8, relative)? as usize;
    let start = command
        .checked_add(string_offset)
        .filter(|start| *start < command_end)
        .ok_or_else(|| format!("embedded runtime Mach-O string is invalid: {relative}"))?;
    let end = bytes[start..command_end]
        .iter()
        .position(|byte| *byte == 0)
        .map(|length| start + length)
        .ok_or_else(|| format!("embedded runtime Mach-O string is unterminated: {relative}"))?;
    std::str::from_utf8(&bytes[start..end])
        .map(str::to_owned)
        .map_err(|_| format!("embedded runtime Mach-O string is invalid UTF-8: {relative}"))
}

fn read_u32(bytes: &[u8], offset: usize, relative: &str) -> Result<u32, String> {
    let end = offset
        .checked_add(4)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| format!("embedded runtime Mach-O is truncated: {relative}"))?;
    Ok(u32::from_le_bytes(
        bytes[offset..end].try_into().expect("four-byte slice"),
    ))
}
