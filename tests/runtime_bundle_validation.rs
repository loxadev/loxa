#![cfg(target_os = "macos")]

use std::fs;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Command;

use loxa::paths::AppPaths;
use loxa::runner::validate_managed_runtime;
use loxa::runtime_bundle::validate_embedded_runtime;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tempfile::tempdir;

const MACH_O_FILES: &[&str] = &[
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

const SYMLINKS: &[(&str, &str)] = &[
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

fn vendor() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("loxa-app/src-tauri/runtime/b10344/upstream")
}

fn sha256(path: &Path) -> String {
    let mut checksum = String::with_capacity(64);
    for byte in Sha256::digest(fs::read(path).unwrap()) {
        use std::fmt::Write as _;
        write!(&mut checksum, "{byte:02x}").unwrap();
    }
    checksum
}

fn command(program: &str, args: &[&str]) {
    let output = Command::new(program).args(args).output().unwrap();
    assert!(
        output.status.success(),
        "{program} {args:?} failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn write_json(path: &Path, value: &Value) {
    let mut bytes = serde_json::to_vec_pretty(value).unwrap();
    bytes.push(b'\n');
    fs::write(path, bytes).unwrap();
}

fn regular_entry(contents: &Path, relative: &str, mach_o: bool) -> Value {
    let path = contents.join(relative);
    json!({
        "path": relative,
        "size": fs::metadata(&path).unwrap().len(),
        "sha256": sha256(&path),
        "mach_o": mach_o,
    })
}

fn refresh_inventory_entry(contents: &Path, relative: &str) {
    let inventory_path = contents.join("Resources/loxa-runtime/b10344/inventory.json");
    let mut inventory: Value = serde_json::from_slice(&fs::read(&inventory_path).unwrap()).unwrap();
    let entries = inventory["regular_files"].as_array_mut().unwrap();
    let entry = entries
        .iter_mut()
        .find(|entry| entry["path"] == relative)
        .unwrap();
    let path = contents.join(relative);
    entry["size"] = fs::metadata(&path).unwrap().len().into();
    entry["sha256"] = sha256(&path).into();
    write_json(&inventory_path, &inventory);
}

fn build_fixture(root: &Path) -> PathBuf {
    let contents = root.join("Loxa.app/Contents");
    let macos = contents.join("MacOS");
    let frameworks = contents.join("Frameworks");
    let resources = contents.join("Resources/loxa-runtime/b10344");
    fs::create_dir_all(&macos).unwrap();
    fs::create_dir(&frameworks).unwrap();
    fs::create_dir_all(&resources).unwrap();
    fs::write(macos.join("Loxa"), b"TEST-APP-MAIN").unwrap();

    fs::copy(vendor().join("llama-server"), macos.join("llama-server")).unwrap();
    fs::set_permissions(
        macos.join("llama-server"),
        fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    for relative in &MACH_O_FILES[1..] {
        let name = Path::new(relative).file_name().unwrap();
        fs::copy(vendor().join(name), frameworks.join(name)).unwrap();
        fs::set_permissions(frameworks.join(name), fs::Permissions::from_mode(0o755)).unwrap();
    }
    for (relative, target) in SYMLINKS {
        symlink(target, contents.join(relative)).unwrap();
    }
    fs::copy(vendor().join("LICENSE"), resources.join("LICENSE")).unwrap();
    fs::copy(
        vendor().join("upstream-provenance.json"),
        resources.join("upstream-provenance.json"),
    )
    .unwrap();

    command(
        "/usr/bin/install_name_tool",
        &[
            "-add_rpath",
            "@executable_path/../Frameworks",
            macos.join("llama-server").to_str().unwrap(),
        ],
    );
    let normalized = json!({
        "schema_version": 1,
        "relocation": "install_name_tool -add_rpath @executable_path/../Frameworks llama-server",
        "regular_files": MACH_O_FILES.iter().map(|relative| {
            regular_entry(&contents, relative, true)
        }).collect::<Vec<_>>(),
    });
    write_json(&resources.join("normalized-inventory.json"), &normalized);

    for relative in MACH_O_FILES.iter().rev() {
        command(
            "/usr/bin/codesign",
            &[
                "--force",
                "--sign",
                "-",
                contents.join(relative).to_str().unwrap(),
            ],
        );
    }

    let mut regular_files = MACH_O_FILES
        .iter()
        .map(|relative| regular_entry(&contents, relative, true))
        .collect::<Vec<_>>();
    for relative in [
        "Resources/loxa-runtime/b10344/LICENSE",
        "Resources/loxa-runtime/b10344/upstream-provenance.json",
        "Resources/loxa-runtime/b10344/normalized-inventory.json",
    ] {
        regular_files.push(regular_entry(&contents, relative, false));
    }
    let inventory = json!({
        "schema_version": 1,
        "runtime": {
            "build": "b10344",
            "commit": "7a20b417f4526cae073bd997af5020cea3e7ccbe",
            "version_line": "version: 10344 (7a20b417f)",
            "architecture": "arm64",
            "minimum_macos": "13.3",
        },
        "regular_files": regular_files,
        "symlinks": SYMLINKS.iter().map(|(path, target)| {
            json!({"path": path, "target": target})
        }).collect::<Vec<_>>(),
    });
    write_json(&resources.join("inventory.json"), &inventory);
    contents
}

fn clone_contents(pristine: &Path, root: &Path) -> PathBuf {
    let target = root.join("Loxa.app/Contents");
    fs::create_dir_all(target.parent().unwrap()).unwrap();
    command(
        "/bin/cp",
        &["-cR", pristine.to_str().unwrap(), target.to_str().unwrap()],
    );
    target
}

fn paths(contents: &Path) -> AppPaths {
    let home = contents.parent().unwrap().parent().unwrap().join("home");
    AppPaths::from_application_values(&contents.join("MacOS/Loxa"), None, Some(&home)).unwrap()
}

fn assert_rejected(contents: &Path, expected: &str) {
    let error = validate_embedded_runtime(&paths(contents)).unwrap_err();
    assert!(
        error.contains(expected),
        "expected {expected:?} in {error:?}"
    );
}

fn patch_minimum_macos_14(path: &Path) {
    let mut bytes = fs::read(path).unwrap();
    assert_eq!(&bytes[..4], &[0xcf, 0xfa, 0xed, 0xfe]);
    let commands = u32::from_le_bytes(bytes[16..20].try_into().unwrap());
    let mut offset = 32_usize;
    let mut patched = false;
    for _ in 0..commands {
        let kind = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        let size = u32::from_le_bytes(bytes[offset + 4..offset + 8].try_into().unwrap()) as usize;
        if kind == 0x32 {
            bytes[offset + 12..offset + 16].copy_from_slice(&(14_u32 << 16).to_le_bytes());
            patched = true;
            break;
        }
        offset += size;
    }
    assert!(patched, "fixture must contain LC_BUILD_VERSION");
    fs::write(path, bytes).unwrap();
}

#[test]
fn embedded_runtime_validates_the_whole_exact_closure_and_rejects_mutations() {
    let pristine_root = tempdir().unwrap();
    let pristine = build_fixture(pristine_root.path());
    validate_embedded_runtime(&paths(&pristine)).unwrap();

    let cases = tempdir().unwrap();

    let missing = clone_contents(&pristine, &cases.path().join("missing"));
    fs::remove_file(missing.join("Frameworks/libggml-rpc.0.19.0.dylib")).unwrap();
    assert_rejected(&missing, "missing");

    let extra = clone_contents(&pristine, &cases.path().join("extra"));
    fs::write(extra.join("Frameworks/libhostile.dylib"), b"HOSTILE").unwrap();
    assert_rejected(&extra, "unexpected");

    let substituted = clone_contents(&pristine, &cases.path().join("substituted"));
    let substituted_path = substituted.join("Frameworks/libggml-rpc.0.19.0.dylib");
    let substituted_size = fs::metadata(&substituted_path).unwrap().len() as usize;
    fs::write(&substituted_path, vec![b'X'; substituted_size]).unwrap();
    assert_rejected(&substituted, "SHA-256");

    let unsafe_link = clone_contents(&pristine, &cases.path().join("unsafe-link"));
    fs::remove_file(unsafe_link.join("Frameworks/libggml.0.dylib")).unwrap();
    symlink(
        "../../outside.dylib",
        unsafe_link.join("Frameworks/libggml.0.dylib"),
    )
    .unwrap();
    assert_rejected(&unsafe_link, "symlink");

    let missing_license = clone_contents(&pristine, &cases.path().join("license"));
    fs::remove_file(missing_license.join("Resources/loxa-runtime/b10344/LICENSE")).unwrap();
    assert_rejected(&missing_license, "LICENSE");

    let provenance = clone_contents(&pristine, &cases.path().join("provenance"));
    let provenance_path = provenance.join("Resources/loxa-runtime/b10344/upstream-provenance.json");
    fs::write(&provenance_path, b"{}\n").unwrap();
    refresh_inventory_entry(
        &provenance,
        "Resources/loxa-runtime/b10344/upstream-provenance.json",
    );
    assert_rejected(&provenance, "provenance");

    let wrong_version = clone_contents(&pristine, &cases.path().join("version"));
    let inventory_path = wrong_version.join("Resources/loxa-runtime/b10344/inventory.json");
    let mut inventory: Value = serde_json::from_slice(&fs::read(&inventory_path).unwrap()).unwrap();
    inventory["runtime"]["version_line"] = "version: 10343 (wrong)".into();
    write_json(&inventory_path, &inventory);
    assert_rejected(&wrong_version, "version");

    let wrong_arch = clone_contents(&pristine, &cases.path().join("architecture"));
    fs::copy("/bin/sh", wrong_arch.join("MacOS/llama-server")).unwrap();
    refresh_inventory_entry(&wrong_arch, "MacOS/llama-server");
    assert_rejected(&wrong_arch, "arm64");

    let wrong_minimum = clone_contents(&pristine, &cases.path().join("minimum"));
    let helper = wrong_minimum.join("MacOS/llama-server");
    patch_minimum_macos_14(&helper);
    command(
        "/usr/bin/codesign",
        &["--force", "--sign", "-", helper.to_str().unwrap()],
    );
    refresh_inventory_entry(&wrong_minimum, "MacOS/llama-server");
    assert_rejected(&wrong_minimum, "minimum macOS");

    let hostile = clone_contents(&pristine, &cases.path().join("hostile-helper"));
    let witness = hostile.join("hostile-helper-executed");
    let helper = hostile.join("MacOS/llama-server");
    fs::write(
        &helper,
        format!(
            "#!/bin/sh\nprintf ran > '{}'\nprintf '%s\\n' 'version: 10344 (7a20b417f)' >&2\n",
            witness.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&helper, fs::Permissions::from_mode(0o755)).unwrap();
    refresh_inventory_entry(&hostile, "MacOS/llama-server");

    let error = validate_managed_runtime(&paths(&hostile)).unwrap_err();
    assert!(error.contains("arm64"), "{error}");
    assert!(
        !witness.exists(),
        "whole-closure validation must run before even the helper version probe"
    );
}
