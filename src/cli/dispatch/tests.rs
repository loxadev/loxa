use super::run_with_recovery;
use crate::cli::Cli;
use crate::paths::AppPaths;
use clap::Parser;

#[test]
fn discovery_commands_bypass_recovery_and_legacy_commands_recover() {
    let temp = tempfile::tempdir().unwrap();
    let paths = AppPaths::from_values(Some(temp.path()), None).unwrap();

    for cli in [
        Cli::parse_from(["loxa", "search", "\n"]),
        Cli::parse_from(["loxa", "inspect", "invalid/repo/shape"]),
    ] {
        let calls = std::cell::Cell::new(0);
        let result = run_with_recovery(cli, paths.clone(), |_| {
            calls.set(calls.get() + 1);
            Err("unexpected recovery".into())
        });

        assert!(result.is_err());
        assert_eq!(calls.get(), 0);
    }

    let calls = std::cell::Cell::new(0);
    let error = run_with_recovery(Cli::parse_from(["loxa", "list"]), paths, |_| {
        calls.set(calls.get() + 1);
        Err("injected combined recovery stop".into())
    })
    .unwrap_err();

    assert_eq!(error, "injected combined recovery stop");
    assert_eq!(calls.get(), 1);
}
