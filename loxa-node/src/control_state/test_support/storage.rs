//! Storage-family fixtures shared by the control-state repository tests.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_ROOT: AtomicU64 = AtomicU64::new(1);

pub(crate) struct TestRoot(PathBuf);

impl TestRoot {
    pub(crate) fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "loxa-control-state-{label}-{}-{}",
            std::process::id(),
            NEXT_ROOT.fetch_add(1, Ordering::Relaxed)
        ));
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(&path).expect("create storage test root");
        Self(fs::canonicalize(path).expect("canonicalize storage test root"))
    }

    pub(crate) fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
        let displaced = self.0.with_extension("displaced");
        let _ = fs::remove_dir_all(displaced);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AuxiliaryKind {
    Wal,
    Journal,
    Backup,
    Shm,
}

impl AuxiliaryKind {
    pub(crate) const ALL: [Self; 4] = [Self::Wal, Self::Journal, Self::Backup, Self::Shm];

    pub(crate) fn path(self, main: &Path) -> PathBuf {
        let suffix = match self {
            Self::Wal => "-wal",
            Self::Journal => "-journal",
            Self::Backup => ".pre-migration.bak",
            Self::Shm => "-shm",
        };
        main.with_file_name(format!(
            "{}{}",
            main.file_name().expect("main filename").to_string_lossy(),
            suffix
        ))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AuxiliaryDefect {
    Symlink,
    NonRegular,
    HardLinked,
    WrongMode,
    WrongOwner,
}

impl AuxiliaryDefect {
    pub(crate) const ALL: [Self; 5] = [
        Self::Symlink,
        Self::NonRegular,
        Self::HardLinked,
        Self::WrongMode,
        Self::WrongOwner,
    ];
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FamilySnapshot {
    main: EntrySnapshot,
    auxiliary: Vec<(AuxiliaryKind, EntrySnapshot)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum EntrySnapshot {
    Missing,
    File {
        bytes: Vec<u8>,
        mode: u32,
        owner: u32,
        links: u64,
        device: u64,
        inode: u64,
    },
    Directory {
        mode: u32,
        owner: u32,
        device: u64,
        inode: u64,
    },
    Symlink {
        target: PathBuf,
        target_snapshot: Box<EntrySnapshot>,
    },
}

pub(crate) fn family_snapshot(main: &Path) -> FamilySnapshot {
    FamilySnapshot {
        main: entry_snapshot(main),
        auxiliary: AuxiliaryKind::ALL
            .into_iter()
            .map(|kind| (kind, entry_snapshot(&kind.path(main))))
            .collect(),
    }
}

#[cfg(unix)]
fn entry_snapshot(path: &Path) -> EntrySnapshot {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let Ok(metadata) = fs::symlink_metadata(path) else {
        return EntrySnapshot::Missing;
    };
    if metadata.file_type().is_symlink() {
        let target = fs::read_link(path).expect("read auxiliary symlink");
        let resolved = if target.is_absolute() {
            target.clone()
        } else {
            path.parent().expect("symlink parent").join(&target)
        };
        return EntrySnapshot::Symlink {
            target,
            target_snapshot: Box::new(entry_snapshot(&resolved)),
        };
    }
    if metadata.file_type().is_dir() {
        return EntrySnapshot::Directory {
            mode: metadata.permissions().mode() & 0o777,
            owner: metadata.uid(),
            device: metadata.dev(),
            inode: metadata.ino(),
        };
    }
    EntrySnapshot::File {
        bytes: fs::read(path).expect("read auxiliary file"),
        mode: metadata.permissions().mode() & 0o777,
        owner: metadata.uid(),
        links: metadata.nlink(),
        device: metadata.dev(),
        inode: metadata.ino(),
    }
}

#[cfg(not(unix))]
fn entry_snapshot(path: &Path) -> EntrySnapshot {
    fs::read(path).map_or(EntrySnapshot::Missing, |bytes| EntrySnapshot::File {
        bytes,
        mode: 0,
        owner: 0,
        links: 1,
        device: 0,
        inode: 0,
    })
}

#[cfg(unix)]
pub(crate) fn apply_auxiliary_defect(main: &Path, kind: AuxiliaryKind, defect: AuxiliaryDefect) {
    use std::os::unix::fs::{symlink, PermissionsExt};

    let path = kind.path(main);
    if defect == AuxiliaryDefect::WrongOwner {
        if !path.exists() {
            fs::write(&path, b"owner-policy-probe").expect("create owner policy probe");
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
                .expect("secure owner policy probe");
        }
        return;
    }
    if let Ok(metadata) = fs::symlink_metadata(&path) {
        if metadata.file_type().is_dir() {
            fs::remove_dir(&path).expect("remove prior auxiliary directory");
        } else {
            fs::remove_file(&path).expect("remove prior auxiliary file");
        }
    }
    let target = path.with_extension(format!(
        "defect-{}",
        NEXT_ROOT.fetch_add(1, Ordering::Relaxed)
    ));
    match defect {
        AuxiliaryDefect::Symlink => {
            fs::write(&target, b"symlink target").expect("create symlink target");
            fs::set_permissions(&target, fs::Permissions::from_mode(0o600))
                .expect("secure symlink target");
            symlink(&target, &path).expect("create auxiliary symlink");
        }
        AuxiliaryDefect::NonRegular => {
            let mut builder = fs::DirBuilder::new();
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
            builder.create(&path).expect("create auxiliary directory");
        }
        AuxiliaryDefect::HardLinked => {
            fs::write(&target, b"hard-link target").expect("create hard-link target");
            fs::set_permissions(&target, fs::Permissions::from_mode(0o600))
                .expect("secure hard-link target");
            fs::hard_link(&target, &path).expect("create auxiliary hard link");
        }
        AuxiliaryDefect::WrongMode => {
            fs::write(&path, b"wrong-mode auxiliary").expect("create wrong-mode auxiliary");
            fs::set_permissions(&path, fs::Permissions::from_mode(0o644))
                .expect("set broad auxiliary mode");
        }
        AuxiliaryDefect::WrongOwner => unreachable!(),
    }
}
