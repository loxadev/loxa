use std::fs;

use loxa::paths::AppPaths;
use loxa::runtime_identity::RuntimeIdentity;
use tempfile::tempdir;

#[test]
fn bundled_application_path_owns_runtime_selection_even_when_the_helper_is_missing() {
    let root = tempdir().unwrap();
    let home = root.path().join("hostile-home");
    let hostile = home.join(".loxa/runtimes/llama.cpp/b10121/llama-server");
    fs::create_dir_all(hostile.parent().unwrap()).unwrap();
    fs::write(&hostile, b"HOSTILE-HOME-RUNTIME").unwrap();

    let contents = root.path().join("Loxa.app/Contents");
    let executable = contents.join("MacOS/Loxa");
    fs::create_dir_all(executable.parent().unwrap()).unwrap();
    fs::write(&executable, b"APP").unwrap();

    let paths = AppPaths::from_application_values(&executable, None, Some(&home)).unwrap();

    assert_eq!(paths.runtime_identity, RuntimeIdentity::BundledB10344);
    assert_eq!(paths.managed_server, contents.join("MacOS/llama-server"));
    assert_eq!(
        paths.runtime_inventory,
        Some(contents.join("Resources/loxa-runtime/b10344/inventory.json"))
    );
    assert_ne!(paths.managed_server, hostile);
    assert_eq!(fs::read(hostile).unwrap(), b"HOSTILE-HOME-RUNTIME");
    assert!(
        !paths.managed_server.exists(),
        "a missing embedded helper must remain a bundled fail-closed selection"
    );
}

#[test]
fn non_bundled_application_keeps_the_explicit_legacy_cli_authority() {
    let root = tempdir().unwrap();
    let home = root.path().join("home");
    let executable = root.path().join("target/debug/loxa-app");

    let paths = AppPaths::from_application_values(&executable, None, Some(&home)).unwrap();

    assert_eq!(paths.runtime_identity, RuntimeIdentity::LegacyCliB10121);
    assert_eq!(
        paths.managed_server,
        home.join(".loxa/runtimes/llama.cpp/b10121/llama-server")
    );
    assert_eq!(paths.runtime_inventory, None);
}
