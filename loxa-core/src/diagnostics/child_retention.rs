use super::DiagnosticsHealth;
use std::collections::HashSet;
use std::ffi::OsString;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

struct InactiveLog {
    path: PathBuf,
    file_name: OsString,
    modified: SystemTime,
}

pub fn prune_inactive_child_logs(
    logs_dir: &Path,
    active: &HashSet<PathBuf>,
    retain_inactive: usize,
    health: &DiagnosticsHealth,
) -> io::Result<()> {
    health.support_retention_failures_counter();

    let result = prune_inactive_child_logs_inner(logs_dir, active, retain_inactive);
    if result.is_err() {
        health.increment_retention_failures();
    }
    result
}

fn prune_inactive_child_logs_inner(
    logs_dir: &Path,
    active: &HashSet<PathBuf>,
    retain_inactive: usize,
) -> io::Result<()> {
    let root_metadata = match fs::symlink_metadata(logs_dir) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if !root_metadata.is_dir() || root_metadata.file_type().is_symlink() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "child log root must be a regular directory",
        ));
    }

    let canonical_root = logs_dir.canonicalize()?;
    let owned_active = active
        .iter()
        .filter_map(|path| normalize_owned_child(&canonical_root, path))
        .collect::<HashSet<_>>();
    let mut inactive = Vec::new();

    for entry in fs::read_dir(&canonical_root)? {
        let entry = entry?;
        let path = entry.path();
        let file_name = entry.file_name();
        if !is_recognized_child_log(&file_name) {
            continue;
        }

        let metadata = fs::symlink_metadata(&path)?;
        if !metadata.file_type().is_file() {
            continue;
        }
        let Some(owned_path) = normalize_owned_child(&canonical_root, &path) else {
            continue;
        };
        if owned_active.contains(&owned_path) {
            continue;
        }

        inactive.push(InactiveLog {
            path,
            file_name,
            modified: metadata.modified()?,
        });
    }

    inactive.sort_by(|left, right| {
        right
            .modified
            .cmp(&left.modified)
            .then_with(|| left.file_name.cmp(&right.file_name))
    });

    for candidate in inactive.into_iter().skip(retain_inactive) {
        let metadata = fs::symlink_metadata(&candidate.path)?;
        if !metadata.file_type().is_file()
            || normalize_owned_child(&canonical_root, &candidate.path).is_none()
        {
            continue;
        }
        fs::remove_file(candidate.path)?;
    }

    Ok(())
}

fn normalize_owned_child(canonical_root: &Path, path: &Path) -> Option<PathBuf> {
    let parent = path.parent()?;
    let file_name = path.file_name()?;
    let canonical_parent = parent.canonicalize().ok()?;
    (canonical_parent == canonical_root).then(|| canonical_root.join(file_name))
}

fn is_recognized_child_log(file_name: &OsString) -> bool {
    let Some(file_name) = file_name.to_str() else {
        return false;
    };
    let Some(stem) = file_name.strip_suffix(".log") else {
        return false;
    };
    let Some((id_and_port, started_at)) = stem.rsplit_once('-') else {
        return false;
    };
    let Some((id, port)) = id_and_port.rsplit_once('-') else {
        return false;
    };

    !id.is_empty()
        && port.parse::<u16>().is_ok_and(|port| port != 0)
        && started_at.parse::<u64>().is_ok()
}

#[cfg(test)]
mod tests {
    use super::prune_inactive_child_logs;
    use crate::diagnostics::DiagnosticsHealth;
    use std::collections::HashSet;
    use std::ffi::OsString;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::thread;
    use std::time::Duration;
    use tempfile::TempDir;

    fn create_log(root: &Path, name: &str, contents: &str) -> PathBuf {
        let path = root.join(name);
        fs::write(&path, contents).expect("create child log");
        path
    }

    #[test]
    fn preserves_active_and_only_seven_newest_inactive_logs() {
        let temp = TempDir::new().expect("tempdir");
        let active = create_log(temp.path(), "active-model-9000-1.log", "active");
        thread::sleep(Duration::from_millis(10));
        let inactive = (2..=9)
            .map(|started_at| {
                let path = create_log(
                    temp.path(),
                    &format!("model-with-hyphens-9000-{started_at}.log"),
                    "inactive",
                );
                thread::sleep(Duration::from_millis(10));
                path
            })
            .collect::<Vec<_>>();

        prune_inactive_child_logs(
            temp.path(),
            &HashSet::from([active.clone()]),
            7,
            &DiagnosticsHealth::new(),
        )
        .expect("prune inactive child logs");

        assert!(active.exists());
        assert!(!inactive[0].exists());
        assert!(inactive[1..].iter().all(|path| path.exists()));
    }

    #[cfg(unix)]
    #[test]
    fn leaves_symlinks_directories_and_unrecognized_entries_untouched() {
        use std::os::unix::fs::symlink;

        let temp = TempDir::new().expect("tempdir");
        let outside = TempDir::new().expect("outside tempdir");
        let outside_target = create_log(outside.path(), "target.log", "outside");
        let symlink_path = temp.path().join("linked-model-9000-1.log");
        symlink(&outside_target, &symlink_path).expect("create symlink");
        let directory = temp.path().join("directory-model-9000-2.log");
        fs::create_dir(&directory).expect("create directory");
        let unrecognized = create_log(temp.path(), "notes.log", "notes");
        let recognized = create_log(temp.path(), "model-with-hyphens-9000-3.log", "recognized");

        prune_inactive_child_logs(temp.path(), &HashSet::new(), 0, &DiagnosticsHealth::new())
            .expect("prune recognized child logs");

        assert!(fs::symlink_metadata(&symlink_path).is_ok());
        assert!(outside_target.exists());
        assert!(directory.is_dir());
        assert!(unrecognized.exists());
        assert!(!recognized.exists());
    }

    #[test]
    fn outside_active_paths_never_preserve_an_inside_candidate() {
        let temp = TempDir::new().expect("tempdir");
        let outside = TempDir::new().expect("outside tempdir");
        let name = "model-with-hyphens-9000-1.log";
        let inside = create_log(temp.path(), name, "inside");
        let outside_active = create_log(outside.path(), name, "outside");

        prune_inactive_child_logs(
            temp.path(),
            &HashSet::from([outside_active.clone()]),
            0,
            &DiagnosticsHealth::new(),
        )
        .expect("prune owned child logs");

        assert!(!inside.exists());
        assert!(outside_active.exists());
    }

    #[test]
    fn reports_root_failures_without_deleting_through_a_symlink() {
        let temp = TempDir::new().expect("tempdir");
        let health = DiagnosticsHealth::new();
        let file_root = temp.path().join("not-a-directory");
        fs::write(&file_root, "file").expect("create non-directory root");

        let error = prune_inactive_child_logs(&file_root, &HashSet::new(), 7, &health)
            .expect_err("reject non-directory root");

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(health.snapshot().retention_failures, Some(1));
        assert!(file_root.exists());
    }

    #[test]
    fn recognizes_numeric_suffixes_without_restricting_hyphenated_ids() {
        assert!(super::is_recognized_child_log(&OsString::from(
            "org-model-family-9000-123.log"
        )));
        assert!(!super::is_recognized_child_log(&OsString::from(
            "org-model-family-port-123.log"
        )));
        assert!(!super::is_recognized_child_log(&OsString::from(
            "org-model-family-9000-started.log"
        )));
        assert!(!super::is_recognized_child_log(&OsString::from(
            "-9000-123.log"
        )));
    }
}
