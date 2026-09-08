use super::entry::{
    open_final_entry, open_invalid_entry, open_part_entry, open_restart_entry, unlink_part_entry,
    unlink_restart_entry,
};
use crate::download::plan::plan_artifact_discard_inner;
use crate::download::{
    ArtifactCheckpoint, ArtifactDiscardError, ArtifactDiscardFacts, ArtifactOperation,
    ArtifactOperationFailure,
};
use crate::safe_file::{
    ensure_directory_descriptor_matches_path, ensure_regular_descriptors_match, DirectoryIdentity,
    RegularFileIdentity,
};
use std::fs::File;
use std::path::Path;

pub(in crate::download) fn discard_artifact_bytes_inner(
    directory: &File,
    model_dir: &Path,
    facts: ArtifactDiscardFacts,
) -> Result<(), ArtifactDiscardError> {
    discard_artifact_bytes_controlled(
        (directory, model_dir),
        facts,
        &mut super::perform_artifact_operation,
    )
}

pub(in crate::download) fn discard_artifact_bytes_controlled(
    directory: (&File, &Path),
    facts: ArtifactDiscardFacts,
    artifact_operation: &mut impl for<'a> FnMut(
        ArtifactOperation<'a>,
    ) -> Result<(), ArtifactOperationFailure>,
) -> Result<(), ArtifactDiscardError> {
    let (directory_file, model_dir) = directory;
    ensure_directory_descriptor_matches_path(directory_file, facts.directory_identity(), model_dir)
        .map_err(|_| ArtifactDiscardError::Changed)?;
    let current = plan_artifact_discard_inner(directory_file, model_dir)?;
    if current != facts {
        return Err(ArtifactDiscardError::Changed);
    }
    let (directory_identity, part, restart) = facts.into_entries();
    let part_path = model_dir.join("model.gguf.part");
    let restart_path = model_dir.join("model.gguf.part.restart");
    let final_path = model_dir.join("model.gguf");
    let invalid_path = model_dir.join("model.gguf.invalid");
    let ensure_current = |part, restart, error| {
        ensure_discard_state(
            (directory_file, &directory_identity, model_dir),
            (part, &part_path),
            (restart, &restart_path),
            (&final_path, &invalid_path),
        )
        .map_err(|_| error)
    };
    let mut deleted = false;
    if restart.is_some() {
        artifact_operation(ArtifactOperation::Observe(
            ArtifactCheckpoint::BeforeRestartUnlink,
        ))
        .map_err(|_| ArtifactDiscardError::Durability)?;
        ensure_current(
            part.as_ref(),
            restart.as_ref(),
            ArtifactDiscardError::Changed,
        )?;
        artifact_operation(ArtifactOperation::Observe(
            ArtifactCheckpoint::RestartIdentityMatched,
        ))
        .map_err(|_| ArtifactDiscardError::Durability)?;
        ensure_current(
            part.as_ref(),
            restart.as_ref(),
            ArtifactDiscardError::Changed,
        )?;
        unlink_restart_entry(directory_file).map_err(|_| ArtifactDiscardError::Durability)?;
        deleted = true;
        artifact_operation(ArtifactOperation::Observe(
            ArtifactCheckpoint::RestartUnlinked,
        ))
        .map_err(|_| ArtifactDiscardError::Durability)?;
        ensure_current(part.as_ref(), None, ArtifactDiscardError::Durability)?;
    }
    if part.is_some() {
        let remaining_restart = if deleted { None } else { restart.as_ref() };
        artifact_operation(ArtifactOperation::Observe(
            ArtifactCheckpoint::BeforeAuthoritativePartUnlink,
        ))
        .map_err(|_| ArtifactDiscardError::Durability)?;
        ensure_current(
            part.as_ref(),
            remaining_restart,
            discard_state_error(deleted),
        )?;
        artifact_operation(ArtifactOperation::Observe(
            ArtifactCheckpoint::AuthoritativePartIdentityMatched,
        ))
        .map_err(|_| ArtifactDiscardError::Durability)?;
        ensure_current(
            part.as_ref(),
            remaining_restart,
            discard_state_error(deleted),
        )?;
        unlink_part_entry(directory_file).map_err(|_| ArtifactDiscardError::Durability)?;
        deleted = true;
        artifact_operation(ArtifactOperation::Observe(
            ArtifactCheckpoint::AuthoritativePartUnlinked,
        ))
        .map_err(|_| ArtifactDiscardError::Durability)?;
        ensure_current(None, remaining_restart, ArtifactDiscardError::Durability)?;
    }
    if !deleted {
        return Ok(());
    }

    artifact_operation(ArtifactOperation::Sync {
        checkpoint: ArtifactCheckpoint::ChecksumCleanupDirectorySynced,
        file: directory_file,
    })
    .map_err(|_| ArtifactDiscardError::Durability)?;
    ensure_current(None, None, ArtifactDiscardError::Durability)?;
    artifact_operation(ArtifactOperation::Observe(
        ArtifactCheckpoint::BeforeAuthoritativePartAbsenceProof,
    ))
    .map_err(|_| ArtifactDiscardError::Durability)?;
    ensure_current(None, None, ArtifactDiscardError::Durability)?;
    artifact_operation(ArtifactOperation::Observe(
        ArtifactCheckpoint::AuthoritativePartAbsent,
    ))
    .map_err(|_| ArtifactDiscardError::Durability)?;
    ensure_current(None, None, ArtifactDiscardError::Durability)
}

fn discard_state_error(deleted: bool) -> ArtifactDiscardError {
    if deleted {
        ArtifactDiscardError::Durability
    } else {
        ArtifactDiscardError::Changed
    }
}

fn ensure_discard_state(
    directory: (&File, &DirectoryIdentity, &Path),
    part: (Option<&RegularFileIdentity>, &Path),
    restart: (Option<&RegularFileIdentity>, &Path),
    forbidden: (&Path, &Path),
) -> Result<(), ()> {
    let (directory, directory_identity, model_dir) = directory;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ())?;
    ensure_discard_entry(part.0, part.1, || open_part_entry(directory, part.1))?;
    ensure_discard_entry(restart.0, restart.1, || {
        open_restart_entry(directory, restart.1)
    })?;
    ensure_discard_entry(None, forbidden.0, || {
        open_final_entry(directory, forbidden.0)
    })?;
    ensure_discard_entry(None, forbidden.1, || {
        open_invalid_entry(directory, forbidden.1)
    })?;
    ensure_directory_descriptor_matches_path(directory, directory_identity, model_dir)
        .map_err(|_| ())
}

fn ensure_discard_entry(
    expected: Option<&RegularFileIdentity>,
    path: &Path,
    open: impl Fn() -> std::io::Result<File>,
) -> Result<(), ()> {
    match (expected, open()) {
        (None, Err(error)) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        (Some(identity), Ok(file)) => {
            let resolved = open().map_err(|_| ())?;
            ensure_regular_descriptors_match(&file, identity, &resolved, path).map_err(|_| ())
        }
        (None, Ok(_)) | (None, Err(_)) | (Some(_), Err(_)) => Err(()),
    }
}
