use super::{DiscardCandidate, TransferError, TransferErrorKind};
use crate::app::AppService;
use crate::catalog::{self, ModelLockError};
use crate::download;
use std::path::Path;

pub(super) fn finish_discard_after_artifact_bytes(
    lock: &crate::catalog::ModelLock,
    model_dir: &Path,
    catalog: catalog::transfer::CatalogDiscardFacts,
) -> Result<(), TransferError> {
    download::plan_artifact_discard(lock.model_directory(), model_dir)
        .map_err(|_| TransferError::terminal(TransferErrorKind::Durability))?;
    lock.revalidate()
        .map_err(|_| TransferError::terminal(TransferErrorKind::Durability))?;
    catalog::transfer::remove_pending_last(lock, catalog)
        .map_err(|_| TransferError::terminal(TransferErrorKind::Durability))
}

impl AppService {
    pub fn prepare_discard(&self, model_id: String) -> Result<DiscardCandidate, TransferError> {
        crate::paths::validate_id(&model_id)
            .map_err(|_| TransferError::terminal(TransferErrorKind::InvalidModelId))?;
        let model_dir = self
            .reader
            .paths
            .model_dir(&model_id)
            .map_err(|_| TransferError::terminal(TransferErrorKind::InvalidModelId))?;
        let lock = catalog::ModelLock::acquire_existing(&model_dir).map_err(|error| {
            TransferError::terminal(match error {
                ModelLockError::Missing => TransferErrorKind::NoIncompleteTransfer,
                ModelLockError::Busy => TransferErrorKind::Busy,
                ModelLockError::UnsafeLocalState => TransferErrorKind::UnsafeLocalState,
            })
        })?;
        let catalog = catalog::transfer::plan_discard(&lock, &model_id).map_err(|error| {
            TransferError::terminal(match error {
                catalog::transfer::CatalogDiscardError::NoIncompleteTransfer => {
                    TransferErrorKind::NoIncompleteTransfer
                }
                catalog::transfer::CatalogDiscardError::InstalledAuthority => {
                    TransferErrorKind::CompletionWon
                }
                catalog::transfer::CatalogDiscardError::ArtifactConflict => {
                    TransferErrorKind::ArtifactConflict
                }
                catalog::transfer::CatalogDiscardError::UnsafeLocalState => {
                    TransferErrorKind::UnsafeLocalState
                }
            })
        })?;
        let artifact = download::plan_artifact_discard(lock.model_directory(), &model_dir)
            .map_err(|error| {
                TransferError::terminal(match error {
                    download::ArtifactDiscardError::Changed => TransferErrorKind::UnsafeLocalState,
                    download::ArtifactDiscardError::Durability => TransferErrorKind::Durability,
                })
            })?;
        lock.revalidate()
            .map_err(|_| TransferError::terminal(TransferErrorKind::UnsafeLocalState))?;
        Ok(DiscardCandidate {
            model_id,
            catalog,
            artifact,
        })
    }

    pub fn discard_transfer(&self, candidate: DiscardCandidate) -> Result<(), TransferError> {
        let DiscardCandidate {
            model_id,
            catalog: captured_catalog,
            artifact: captured_artifact,
        } = candidate;
        let model_dir = self
            .reader
            .paths
            .model_dir(&model_id)
            .map_err(|_| TransferError::terminal(TransferErrorKind::InvalidModelId))?;
        let lock = catalog::ModelLock::acquire_existing(&model_dir).map_err(|error| {
            TransferError::terminal(match error {
                ModelLockError::Busy => TransferErrorKind::Busy,
                ModelLockError::Missing | ModelLockError::UnsafeLocalState => {
                    TransferErrorKind::IncompleteTransferChanged
                }
            })
        })?;
        let current_catalog = catalog::transfer::plan_discard(&lock, &model_id)
            .map_err(|_| TransferError::terminal(TransferErrorKind::IncompleteTransferChanged))?;
        if current_catalog != captured_catalog {
            return Err(TransferError::terminal(
                TransferErrorKind::IncompleteTransferChanged,
            ));
        }
        let current_artifact = download::plan_artifact_discard(lock.model_directory(), &model_dir)
            .map_err(|_| TransferError::terminal(TransferErrorKind::IncompleteTransferChanged))?;
        if current_artifact != captured_artifact {
            return Err(TransferError::terminal(
                TransferErrorKind::IncompleteTransferChanged,
            ));
        }
        lock.revalidate()
            .map_err(|_| TransferError::terminal(TransferErrorKind::IncompleteTransferChanged))?;
        download::discard_artifact_bytes(lock.model_directory(), &model_dir, captured_artifact)
            .map_err(|error| {
                TransferError::terminal(match error {
                    download::ArtifactDiscardError::Changed => {
                        TransferErrorKind::IncompleteTransferChanged
                    }
                    download::ArtifactDiscardError::Durability => TransferErrorKind::Durability,
                })
            })?;
        finish_discard_after_artifact_bytes(&lock, &model_dir, captured_catalog)
    }
}
