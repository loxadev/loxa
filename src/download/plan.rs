use super::{ArtifactDiscardError, ArtifactDiscardFacts};
use crate::safe_file::{
    directory_identity, ensure_directory_descriptor_matches_path, ensure_regular_descriptors_match,
    regular_file_identity, RegularFileIdentity,
};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ArtifactTransferState {
    Fresh,
    ValidFinal,
    ValidFinalRepairDebris,
    CompletePart,
    RequestCapable,
    InstalledRepair,
    Unsafe,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ArtifactPlanOutcome {
    Ready(Box<ArtifactTransferPlan>),
    Interrupted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ArtifactEntry {
    identity: RegularFileIdentity,
    length: u64,
    checksum_matches: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ArtifactTransferPlan {
    state: ArtifactTransferState,
    final_entry: Option<ArtifactEntry>,
    part: Option<ArtifactEntry>,
    restart: Option<ArtifactEntry>,
    invalid: Option<ArtifactEntry>,
}

impl ArtifactTransferPlan {
    pub(crate) fn state(&self) -> ArtifactTransferState {
        self.state
    }

    pub(crate) fn part_length(&self) -> Option<u64> {
        self.part.as_ref().map(|entry| entry.length)
    }

    pub(crate) fn restart_length(&self) -> Option<u64> {
        self.restart.as_ref().map(|entry| entry.length)
    }

    pub(crate) fn has_invalid_authority(&self) -> bool {
        self.invalid.is_some()
    }

    pub(crate) fn invalid_length(&self) -> Option<u64> {
        self.invalid.as_ref().map(|entry| entry.length)
    }
}

pub(crate) fn plan_artifact_transfer(
    directory: &File,
    model_dir: &Path,
    expected_size: u64,
    expected_sha256: &str,
    should_pause: &impl Fn() -> bool,
) -> ArtifactPlanOutcome {
    plan_artifact_transfer_with_after_open(
        directory,
        model_dir,
        expected_size,
        expected_sha256,
        should_pause,
        |_| {},
    )
}

fn plan_artifact_transfer_with_after_open(
    directory: &File,
    model_dir: &Path,
    expected_size: u64,
    expected_sha256: &str,
    should_pause: &impl Fn() -> bool,
    mut after_open: impl FnMut(ArtifactName),
) -> ArtifactPlanOutcome {
    match plan_artifact_transfer_inner(
        directory,
        model_dir,
        expected_size,
        expected_sha256,
        should_pause,
        &mut after_open,
    ) {
        Ok(plan) => ArtifactPlanOutcome::Ready(Box::new(plan)),
        Err(PlanReadError::Interrupted) => ArtifactPlanOutcome::Interrupted,
        Err(PlanReadError::Unsafe) => ArtifactPlanOutcome::Ready(Box::new(ArtifactTransferPlan {
            state: ArtifactTransferState::Unsafe,
            final_entry: None,
            part: None,
            restart: None,
            invalid: None,
        })),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PlanReadError {
    Interrupted,
    Unsafe,
}

fn plan_artifact_transfer_inner(
    directory: &File,
    model_dir: &Path,
    expected_size: u64,
    expected_sha256: &str,
    should_pause: &impl Fn() -> bool,
    after_open: &mut impl FnMut(ArtifactName),
) -> Result<ArtifactTransferPlan, PlanReadError> {
    let final_entry = read_entry(
        directory,
        model_dir,
        ArtifactName::Final,
        expected_size,
        expected_sha256,
        should_pause,
        after_open,
    )?;
    let part = read_entry(
        directory,
        model_dir,
        ArtifactName::Part,
        expected_size,
        expected_sha256,
        should_pause,
        after_open,
    )?;
    let restart = read_entry(
        directory,
        model_dir,
        ArtifactName::Restart,
        expected_size,
        expected_sha256,
        should_pause,
        after_open,
    )?;
    let invalid = read_entry(
        directory,
        model_dir,
        ArtifactName::Invalid,
        expected_size,
        expected_sha256,
        should_pause,
        after_open,
    )?;

    let state = if final_entry
        .as_ref()
        .is_some_and(|entry| entry.length == expected_size && entry.checksum_matches)
    {
        if part.is_some() || restart.is_some() || invalid.is_some() {
            ArtifactTransferState::ValidFinalRepairDebris
        } else {
            ArtifactTransferState::ValidFinal
        }
    } else if final_entry.is_some() || invalid.is_some() {
        ArtifactTransferState::InstalledRepair
    } else if part
        .as_ref()
        .is_some_and(|entry| entry.length == expected_size && entry.checksum_matches)
    {
        ArtifactTransferState::CompletePart
    } else if part.is_none() && restart.is_none() {
        ArtifactTransferState::Fresh
    } else {
        ArtifactTransferState::RequestCapable
    };
    Ok(ArtifactTransferPlan {
        state,
        final_entry,
        part,
        restart,
        invalid,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ArtifactName {
    Final,
    Part,
    Restart,
    Invalid,
}

impl ArtifactName {
    fn path(self, directory: &Path) -> std::path::PathBuf {
        directory.join(match self {
            Self::Final => "model.gguf",
            Self::Part => "model.gguf.part",
            Self::Restart => "model.gguf.part.restart",
            Self::Invalid => "model.gguf.invalid",
        })
    }

    fn needs_checksum(self) -> bool {
        matches!(self, Self::Final | Self::Part)
    }

    #[cfg(unix)]
    fn c_name(self) -> &'static std::ffi::CStr {
        match self {
            Self::Final => c"model.gguf",
            Self::Part => c"model.gguf.part",
            Self::Restart => c"model.gguf.part.restart",
            Self::Invalid => c"model.gguf.invalid",
        }
    }
}

fn read_entry(
    directory: &File,
    model_dir: &Path,
    name: ArtifactName,
    expected_size: u64,
    expected_sha256: &str,
    should_pause: &impl Fn() -> bool,
    after_open: &mut impl FnMut(ArtifactName),
) -> Result<Option<ArtifactEntry>, PlanReadError> {
    let path = name.path(model_dir);
    let Some(mut file) = open_entry(directory, &path, name).map_err(|_| PlanReadError::Unsafe)?
    else {
        return Ok(None);
    };
    let identity = regular_file_identity(&file, &path).map_err(|_| PlanReadError::Unsafe)?;
    after_open(name);
    let length = file.metadata().map_err(|_| PlanReadError::Unsafe)?.len();
    let checksum_matches = if name.needs_checksum() && length == expected_size {
        let mut hash = Sha256::new();
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            if should_pause() {
                return Err(PlanReadError::Interrupted);
            }
            let read = file.read(&mut buffer).map_err(|_| PlanReadError::Unsafe)?;
            if read == 0 {
                break;
            }
            hash.update(&buffer[..read]);
        }
        let actual = crate::download::artifact::hex(hash.finalize().as_ref());
        actual == expected_sha256.to_ascii_lowercase()
    } else {
        false
    };
    let resolved = open_entry(directory, &path, name)
        .map_err(|_| PlanReadError::Unsafe)?
        .ok_or(PlanReadError::Unsafe)?;
    ensure_regular_descriptors_match(&file, &identity, &resolved, &path)
        .map_err(|_| PlanReadError::Unsafe)?;
    Ok(Some(ArtifactEntry {
        identity,
        length,
        checksum_matches,
    }))
}

#[cfg(unix)]
fn open_entry(directory: &File, _path: &Path, name: ArtifactName) -> io::Result<Option<File>> {
    use std::os::fd::{AsRawFd, FromRawFd};

    let descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.c_name().as_ptr(),
            libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_RDONLY,
        )
    };
    if descriptor == -1 {
        let error = io::Error::last_os_error();
        return if error.kind() == io::ErrorKind::NotFound {
            Ok(None)
        } else {
            Err(error)
        };
    }
    Ok(Some(unsafe { File::from_raw_fd(descriptor) }))
}

pub(super) fn plan_artifact_discard_inner(
    directory: &File,
    model_dir: &Path,
) -> Result<ArtifactDiscardFacts, ArtifactDiscardError> {
    let directory_identity =
        directory_identity(directory, model_dir).map_err(|_| ArtifactDiscardError::Changed)?;
    ensure_directory_descriptor_matches_path(directory, &directory_identity, model_dir)
        .map_err(|_| ArtifactDiscardError::Changed)?;
    ensure_discard_entry_absent(directory, model_dir, ArtifactName::Final)?;
    let part = capture_discard_entry(directory, model_dir, ArtifactName::Part)?;
    let restart = capture_discard_entry(directory, model_dir, ArtifactName::Restart)?;
    ensure_discard_entry_absent(directory, model_dir, ArtifactName::Invalid)?;
    ensure_discard_entry_current(directory, model_dir, ArtifactName::Part, part.as_ref())?;
    ensure_discard_entry_current(
        directory,
        model_dir,
        ArtifactName::Restart,
        restart.as_ref(),
    )?;
    ensure_discard_entry_absent(directory, model_dir, ArtifactName::Final)?;
    ensure_discard_entry_absent(directory, model_dir, ArtifactName::Invalid)?;
    ensure_directory_descriptor_matches_path(directory, &directory_identity, model_dir)
        .map_err(|_| ArtifactDiscardError::Changed)?;
    Ok(ArtifactDiscardFacts {
        directory_identity,
        part,
        restart,
    })
}

fn capture_discard_entry(
    directory: &File,
    model_dir: &Path,
    name: ArtifactName,
) -> Result<Option<RegularFileIdentity>, ArtifactDiscardError> {
    let path = name.path(model_dir);
    let Some(file) =
        open_entry(directory, &path, name).map_err(|_| ArtifactDiscardError::Changed)?
    else {
        return Ok(None);
    };
    let identity =
        regular_file_identity(&file, &path).map_err(|_| ArtifactDiscardError::Changed)?;
    let resolved = open_entry(directory, &path, name)
        .map_err(|_| ArtifactDiscardError::Changed)?
        .ok_or(ArtifactDiscardError::Changed)?;
    ensure_regular_descriptors_match(&file, &identity, &resolved, &path)
        .map_err(|_| ArtifactDiscardError::Changed)?;
    Ok(Some(identity))
}

fn ensure_discard_entry_current(
    directory: &File,
    model_dir: &Path,
    name: ArtifactName,
    expected: Option<&RegularFileIdentity>,
) -> Result<(), ArtifactDiscardError> {
    let path = name.path(model_dir);
    match (expected, open_entry(directory, &path, name)) {
        (None, Ok(None)) => Ok(()),
        (Some(identity), Ok(Some(file))) => {
            let resolved = open_entry(directory, &path, name)
                .map_err(|_| ArtifactDiscardError::Changed)?
                .ok_or(ArtifactDiscardError::Changed)?;
            ensure_regular_descriptors_match(&file, identity, &resolved, &path)
                .map_err(|_| ArtifactDiscardError::Changed)
        }
        (None, Ok(Some(_))) | (Some(_), Ok(None)) | (_, Err(_)) => {
            Err(ArtifactDiscardError::Changed)
        }
    }
}

fn ensure_discard_entry_absent(
    directory: &File,
    model_dir: &Path,
    name: ArtifactName,
) -> Result<(), ArtifactDiscardError> {
    match open_entry(directory, &name.path(model_dir), name) {
        Ok(None) => Ok(()),
        Ok(Some(_)) | Err(_) => Err(ArtifactDiscardError::Changed),
    }
}

#[cfg(not(unix))]
fn open_entry(_directory: &File, path: &Path, _name: ArtifactName) -> io::Result<Option<File>> {
    match crate::safe_file::open_regular_file(path) {
        Ok((file, _)) => Ok(Some(file)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    fn checksum(bytes: &[u8]) -> String {
        crate::download::artifact::hex(Sha256::digest(bytes).as_ref())
    }

    fn open_directory(path: &std::path::Path) -> std::fs::File {
        crate::safe_file::open_directory(path).unwrap().0
    }

    fn ready_plan(
        directory: &std::fs::File,
        model_dir: &std::path::Path,
        expected_size: u64,
        expected_sha256: &str,
    ) -> ArtifactTransferPlan {
        match plan_artifact_transfer(
            directory,
            model_dir,
            expected_size,
            expected_sha256,
            &|| false,
        ) {
            ArtifactPlanOutcome::Ready(plan) => *plan,
            ArtifactPlanOutcome::Interrupted => panic!("inert control interrupted artifact plan"),
        }
    }

    #[derive(Debug, Eq, PartialEq)]
    struct SnapshotEntry {
        name: String,
        bytes: Vec<u8>,
        #[cfg(unix)]
        device: u64,
        #[cfg(unix)]
        inode: u64,
        #[cfg(unix)]
        links: u64,
    }

    fn snapshot(path: &std::path::Path) -> Vec<SnapshotEntry> {
        let mut entries = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                let metadata = entry.metadata().unwrap();
                #[cfg(unix)]
                use std::os::unix::fs::MetadataExt;
                SnapshotEntry {
                    name: entry.file_name().into_string().unwrap(),
                    bytes: std::fs::read(entry.path()).unwrap(),
                    #[cfg(unix)]
                    device: metadata.dev(),
                    #[cfg(unix)]
                    inode: metadata.ino(),
                    #[cfg(unix)]
                    links: metadata.nlink(),
                }
            })
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.name.cmp(&right.name));
        entries
    }

    #[test]
    fn artifact_plan_classifies_clean_fresh_valid_final_complete_part_and_request_capable_states() {
        let bytes = b"abcdef";
        let sha256 = checksum(bytes);

        let fresh = tempfile::tempdir().unwrap();
        let fresh_directory = open_directory(fresh.path());
        let before = snapshot(fresh.path());
        let plan = ready_plan(&fresh_directory, fresh.path(), 6, &sha256);
        assert_eq!(plan.state(), ArtifactTransferState::Fresh);
        assert_eq!(snapshot(fresh.path()), before);

        let installed = tempfile::tempdir().unwrap();
        std::fs::write(installed.path().join("model.gguf"), bytes).unwrap();
        let installed_directory = open_directory(installed.path());
        let before = snapshot(installed.path());
        let plan = ready_plan(&installed_directory, installed.path(), 6, &sha256);
        assert_eq!(plan.state(), ArtifactTransferState::ValidFinal);
        assert_eq!(snapshot(installed.path()), before);

        let complete_part = tempfile::tempdir().unwrap();
        std::fs::write(complete_part.path().join("model.gguf.part"), bytes).unwrap();
        let complete_directory = open_directory(complete_part.path());
        let before = snapshot(complete_part.path());
        let plan = ready_plan(&complete_directory, complete_part.path(), 6, &sha256);
        assert_eq!(plan.state(), ArtifactTransferState::CompletePart);
        assert_eq!(snapshot(complete_part.path()), before);

        for part in [b"".as_slice(), b"abc", b"abcdefg"] {
            let request = tempfile::tempdir().unwrap();
            std::fs::write(request.path().join("model.gguf.part"), part).unwrap();
            let request_directory = open_directory(request.path());
            let before = snapshot(request.path());
            let plan = ready_plan(&request_directory, request.path(), 6, &sha256);
            assert_eq!(plan.state(), ArtifactTransferState::RequestCapable);
            assert_eq!(snapshot(request.path()), before);
        }
    }

    #[test]
    fn artifact_plan_classifies_exact_installed_repair_without_claiming_catalog_authority() {
        let bytes = b"abcdef";
        let sha256 = checksum(bytes);

        let corrupt_final = tempfile::tempdir().unwrap();
        std::fs::write(corrupt_final.path().join("model.gguf"), b"abcdeg").unwrap();
        let directory = open_directory(corrupt_final.path());
        let plan = ready_plan(&directory, corrupt_final.path(), 6, &sha256);
        assert_eq!(plan.state(), ArtifactTransferState::InstalledRepair);

        let invalid = tempfile::tempdir().unwrap();
        std::fs::write(invalid.path().join("model.gguf"), bytes).unwrap();
        std::fs::write(invalid.path().join("model.gguf.invalid"), b"corrupt").unwrap();
        std::fs::write(invalid.path().join("model.gguf.part.restart"), b"stale").unwrap();
        std::fs::write(invalid.path().join("pending.json"), b"opaque catalog bytes").unwrap();
        let directory = open_directory(invalid.path());
        let before = snapshot(invalid.path());
        let plan = ready_plan(&directory, invalid.path(), 6, &sha256);
        assert_eq!(plan.state(), ArtifactTransferState::ValidFinalRepairDebris);
        assert_eq!(snapshot(invalid.path()), before);
    }

    #[test]
    fn artifact_plan_preserves_zero_partial_exact_and_oversize_part_restart_invalid_lengths_read_only(
    ) {
        let expected = b"abcdef";
        let sha256 = checksum(expected);
        for bytes in [
            b"".as_slice(),
            b"abc".as_slice(),
            expected.as_slice(),
            b"abcdefg".as_slice(),
        ] {
            let part_root = tempfile::tempdir().unwrap();
            std::fs::write(part_root.path().join("model.gguf.part"), bytes).unwrap();
            let directory = open_directory(part_root.path());
            let before = snapshot(part_root.path());
            let plan = ready_plan(&directory, part_root.path(), 6, &sha256);
            assert_eq!(plan.part_length(), Some(bytes.len() as u64));
            assert_eq!(snapshot(part_root.path()), before);

            let restart_root = tempfile::tempdir().unwrap();
            std::fs::write(restart_root.path().join("model.gguf.part.restart"), bytes).unwrap();
            let directory = open_directory(restart_root.path());
            let before = snapshot(restart_root.path());
            let plan = ready_plan(&directory, restart_root.path(), 6, &sha256);
            assert_eq!(plan.restart_length(), Some(bytes.len() as u64));
            assert_eq!(snapshot(restart_root.path()), before);

            let invalid_root = tempfile::tempdir().unwrap();
            std::fs::write(invalid_root.path().join("model.gguf.invalid"), bytes).unwrap();
            let directory = open_directory(invalid_root.path());
            let before = snapshot(invalid_root.path());
            let plan = ready_plan(&directory, invalid_root.path(), 6, &sha256);
            assert_eq!(plan.invalid_length(), Some(bytes.len() as u64));
            assert!(plan.has_invalid_authority());
            assert!(
                !plan.invalid.as_ref().unwrap().checksum_matches,
                "invalid repair evidence must never be interpreted as verified content"
            );
            assert_eq!(snapshot(invalid_root.path()), before);
        }
    }

    #[cfg(unix)]
    fn create_fifo(path: &std::path::Path) {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let path = CString::new(path.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
    }

    #[cfg(unix)]
    fn release_fifo_reader(path: &std::path::Path) {
        use std::os::unix::fs::OpenOptionsExt;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        loop {
            match std::fs::OpenOptions::new()
                .write(true)
                .custom_flags(libc::O_NONBLOCK)
                .open(path)
            {
                Ok(writer) => {
                    drop(writer);
                    return;
                }
                Err(error)
                    if error.raw_os_error() == Some(libc::ENXIO)
                        && std::time::Instant::now() < deadline =>
                {
                    std::thread::yield_now();
                }
                Err(error) => panic!("failed to release FIFO reader: {error}"),
            }
        }
    }

    #[cfg(unix)]
    fn fifo_plan_returns_without_waiting(
        path: &std::path::Path,
        audit: impl FnOnce() -> ArtifactTransferPlan + Send + 'static,
    ) -> (bool, ArtifactTransferPlan) {
        use std::sync::mpsc::{self, RecvTimeoutError};
        use std::time::Duration;

        let (sender, receiver) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            let _ = sender.send(audit());
        });
        let first = receiver.recv_timeout(Duration::from_millis(250));
        let returned_without_waiting = first.is_ok();
        if matches!(first, Err(RecvTimeoutError::Timeout)) {
            release_fifo_reader(path);
        }
        let (plan, finished) = match first {
            Ok(plan) => (plan, true),
            Err(RecvTimeoutError::Timeout) => match receiver.recv_timeout(Duration::from_secs(1)) {
                Ok(plan) => (plan, true),
                Err(RecvTimeoutError::Disconnected) => panic!("FIFO audit worker disconnected"),
                Err(RecvTimeoutError::Timeout) => {
                    drop(worker);
                    panic!("released FIFO audit did not finish")
                }
            },
            Err(RecvTimeoutError::Disconnected) => panic!("FIFO audit worker disconnected"),
        };
        if finished {
            worker.join().unwrap();
        }
        (returned_without_waiting, plan)
    }

    #[cfg(unix)]
    fn unix_node_identity(path: &std::path::Path) -> (u64, u64, u32, u64, u64) {
        use std::os::unix::fs::MetadataExt;
        let metadata = std::fs::symlink_metadata(path).unwrap();
        (
            metadata.dev(),
            metadata.ino(),
            metadata.mode(),
            metadata.nlink(),
            metadata.len(),
        )
    }

    #[test]
    fn artifact_plan_rejects_symlink_hard_link_nonregular_and_substituted_managed_entries() {
        #[cfg(unix)]
        {
            use std::cell::Cell;
            use std::os::unix::fs::symlink;

            let expected = b"abcdef";
            let sha256 = checksum(expected);

            let symlink_root = tempfile::tempdir().unwrap();
            let outside = symlink_root.path().join("outside");
            std::fs::write(&outside, expected).unwrap();
            let linked = symlink_root.path().join("model.gguf");
            symlink(&outside, &linked).unwrap();
            let before = unix_node_identity(&linked);
            let directory = open_directory(symlink_root.path());
            let plan = ready_plan(&directory, symlink_root.path(), 6, &sha256);
            assert_eq!(plan.state(), ArtifactTransferState::Unsafe);
            assert_eq!(unix_node_identity(&linked), before);
            assert_eq!(std::fs::read(&outside).unwrap(), expected);

            let hardlink_root = tempfile::tempdir().unwrap();
            let outside = hardlink_root.path().join("outside");
            std::fs::write(&outside, expected).unwrap();
            let linked = hardlink_root.path().join("model.gguf.part");
            std::fs::hard_link(&outside, &linked).unwrap();
            let before = unix_node_identity(&linked);
            let directory = open_directory(hardlink_root.path());
            let plan = ready_plan(&directory, hardlink_root.path(), 6, &sha256);
            assert_eq!(plan.state(), ArtifactTransferState::Unsafe);
            assert_eq!(unix_node_identity(&linked), before);
            assert_eq!(std::fs::read(&outside).unwrap(), expected);

            let directory_root = tempfile::tempdir().unwrap();
            let managed = directory_root.path().join("model.gguf.part.restart");
            std::fs::create_dir(&managed).unwrap();
            let before = unix_node_identity(&managed);
            let directory = open_directory(directory_root.path());
            let plan = ready_plan(&directory, directory_root.path(), 6, &sha256);
            assert_eq!(plan.state(), ArtifactTransferState::Unsafe);
            assert_eq!(unix_node_identity(&managed), before);

            for name in [
                ArtifactName::Final,
                ArtifactName::Part,
                ArtifactName::Restart,
                ArtifactName::Invalid,
            ] {
                let fifo_root = tempfile::tempdir().unwrap();
                let fifo = name.path(fifo_root.path());
                create_fifo(&fifo);
                let before = unix_node_identity(&fifo);
                let directory = open_directory(fifo_root.path());
                let root = fifo_root.path().to_path_buf();
                let sha256 = sha256.clone();
                let (returned_without_waiting, plan) =
                    fifo_plan_returns_without_waiting(&fifo, move || {
                        ready_plan(&directory, &root, 6, &sha256)
                    });
                assert!(
                    returned_without_waiting,
                    "artifact audit waited on {name:?}"
                );
                assert_eq!(plan.state(), ArtifactTransferState::Unsafe);
                assert_eq!(unix_node_identity(&fifo), before);
            }

            let substituted = tempfile::tempdir().unwrap();
            let target = substituted.path().join("model.gguf");
            let replacement = substituted.path().join("replacement");
            let swap = substituted.path().join("swap");
            std::fs::write(&target, expected).unwrap();
            std::fs::write(&replacement, expected).unwrap();
            let before = snapshot(substituted.path());
            let called = Cell::new(false);
            let directory = open_directory(substituted.path());
            let plan = match plan_artifact_transfer_with_after_open(
                &directory,
                substituted.path(),
                6,
                &sha256,
                &|| false,
                |name| {
                    if name == ArtifactName::Final {
                        std::fs::rename(&target, &swap).unwrap();
                        std::fs::rename(&replacement, &target).unwrap();
                        std::fs::rename(&swap, &replacement).unwrap();
                        called.set(true);
                    }
                },
            ) {
                ArtifactPlanOutcome::Ready(plan) => plan,
                ArtifactPlanOutcome::Interrupted => panic!("inert control interrupted audit"),
            };
            assert!(called.get());
            std::fs::rename(&target, &swap).unwrap();
            std::fs::rename(&replacement, &target).unwrap();
            std::fs::rename(&swap, &replacement).unwrap();
            assert_eq!(plan.state(), ArtifactTransferState::Unsafe);
            assert_eq!(snapshot(substituted.path()), before);

            let rewritten = tempfile::tempdir().unwrap();
            let part = rewritten.path().join("model.gguf.part");
            std::fs::write(&part, expected).unwrap();
            let original_modified = std::fs::metadata(&part).unwrap().modified().unwrap();
            let directory = open_directory(rewritten.path());
            let plan = match plan_artifact_transfer_with_after_open(
                &directory,
                rewritten.path(),
                6,
                &sha256,
                &|| false,
                |name| {
                    if name == ArtifactName::Part {
                        std::fs::write(&part, b"abcdeg").unwrap();
                        std::fs::OpenOptions::new()
                            .write(true)
                            .open(&part)
                            .unwrap()
                            .set_times(std::fs::FileTimes::new().set_modified(original_modified))
                            .unwrap();
                    }
                },
            ) {
                ArtifactPlanOutcome::Ready(plan) => plan,
                ArtifactPlanOutcome::Interrupted => panic!("inert control interrupted audit"),
            };
            assert_eq!(plan.state(), ArtifactTransferState::Unsafe);
        }
    }

    #[test]
    fn artifact_plan_never_reads_pending_manifest_or_catalog_temps() {
        #[cfg(unix)]
        {
            let sha256 = checksum(b"abcdef");
            for name in [
                "pending.json",
                "manifest.json",
                "pending.json.tmp",
                "manifest.json.tmp",
            ] {
                let root = tempfile::tempdir().unwrap();
                let foreign = root.path().join(name);
                create_fifo(&foreign);
                let before = unix_node_identity(&foreign);
                let directory = open_directory(root.path());
                let model_dir = root.path().to_path_buf();
                let sha256 = sha256.clone();
                let (returned_without_waiting, plan) =
                    fifo_plan_returns_without_waiting(&foreign, move || {
                        ready_plan(&directory, &model_dir, 6, &sha256)
                    });
                assert!(returned_without_waiting, "artifact audit read {name}");
                assert_eq!(plan.state(), ArtifactTransferState::Fresh);
                assert_eq!(unix_node_identity(&foreign), before);
            }
        }
    }
}
