use super::AppService;
use std::ffi::OsStr;
use std::fs;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncompleteTransferSummary {
    model_id: String,
    completed_bytes: u64,
    total_bytes: u64,
}

impl IncompleteTransferSummary {
    pub(crate) fn new(model_id: String, completed_bytes: u64, total_bytes: u64) -> Self {
        Self {
            model_id,
            completed_bytes,
            total_bytes,
        }
    }

    pub fn model_id(&self) -> &str {
        &self.model_id
    }

    pub fn completed_bytes(&self) -> u64 {
        self.completed_bytes
    }

    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }
}

pub struct IncompleteTransferInventory {
    entries: Vec<IncompleteTransferSummary>,
    unrecognized_root_partials: usize,
}

impl IncompleteTransferInventory {
    pub fn entries(&self) -> &[IncompleteTransferSummary] {
        &self.entries
    }

    pub fn unrecognized_root_partials(&self) -> usize {
        self.unrecognized_root_partials
    }
}

impl AppService {
    pub fn incomplete_transfers(&self) -> Result<IncompleteTransferInventory, String> {
        let models_root = &self.reader.paths.models;
        let entries = match fs::read_dir(models_root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(IncompleteTransferInventory {
                    entries: Vec::new(),
                    unrecognized_root_partials: 0,
                });
            }
            Err(error) => return Err(error.to_string()),
        };

        let mut incomplete = Vec::new();
        let mut unrecognized_root_partials = 0usize;
        for entry in entries {
            let Ok(entry) = entry else {
                continue;
            };
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_file() && entry.path().extension() == Some(OsStr::new("part")) {
                unrecognized_root_partials = unrecognized_root_partials.saturating_add(1);
                continue;
            }
            if !file_type.is_dir() {
                continue;
            }
            let Some(model_id) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if crate::paths::validate_id(&model_id).is_err() {
                continue;
            }
            let Ok(candidate) = self.prepare_discard(model_id.clone()) else {
                continue;
            };
            let completed_bytes = candidate.retained_bytes();
            let total_bytes = candidate.total_bytes();
            if total_bytes == 0 || completed_bytes > total_bytes {
                continue;
            }
            incomplete.push(IncompleteTransferSummary::new(
                model_id,
                completed_bytes,
                total_bytes,
            ));
        }
        incomplete.sort_by(|left, right| left.model_id.cmp(&right.model_id));
        Ok(IncompleteTransferInventory {
            entries: incomplete,
            unrecognized_root_partials,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Manifest;
    use crate::paths::AppPaths;
    use std::fs;

    fn manifest(id: &str, size: u64) -> Manifest {
        Manifest {
            version: 1,
            id: id.into(),
            repo: Some("owner/repo".into()),
            revision: Some("a".repeat(40)),
            remote_filename: Some("model.gguf".into()),
            origin: None,
            source_filename: None,
            local_filename: "model.gguf".into(),
            sha256: "b".repeat(64),
            size,
            artifacts: None,
            profile: None,
            runtime: None,
        }
    }

    fn service(root: &tempfile::TempDir) -> (AppService, AppPaths) {
        let paths = AppPaths::from_values(Some(root.path()), None).unwrap();
        (AppService::from_paths(paths.clone()), paths)
    }

    fn pending(paths: &AppPaths, id: &str, total: u64) {
        let model_dir = paths.model_dir(id).unwrap();
        let lock = crate::catalog::ModelLock::acquire(&model_dir).unwrap();
        drop(lock);
        crate::catalog::prepare_pull(&model_dir, &manifest(id, total)).unwrap();
    }

    #[test]
    fn inventory_reports_only_admitted_managed_pending_transfers_without_hashing_or_mutation() {
        let root = tempfile::tempdir().unwrap();
        let (service, paths) = service(&root);
        pending(&paths, "alpha", 100);
        fs::write(
            paths.model_dir("alpha").unwrap().join("model.gguf.part"),
            vec![0; 23],
        )
        .unwrap();
        fs::write(
            paths
                .model_dir("alpha")
                .unwrap()
                .join("model.gguf.part.restart"),
            vec![0; 31],
        )
        .unwrap();
        pending(&paths, "zero", 200);

        let malformed = paths.models.join("malformed");
        fs::create_dir_all(&malformed).unwrap();
        fs::write(malformed.join(".lock"), []).unwrap();
        fs::write(malformed.join("pending.json"), b"{").unwrap();
        fs::write(malformed.join("model.gguf.part"), vec![0; 99]).unwrap();

        pending(&paths, "installed-won", 300);
        fs::copy(
            paths
                .model_dir("installed-won")
                .unwrap()
                .join("pending.json"),
            paths
                .model_dir("installed-won")
                .unwrap()
                .join("manifest.json"),
        )
        .unwrap();

        pending(&paths, "busy", 400);
        let busy =
            crate::catalog::ModelLock::acquire_existing(&paths.model_dir("busy").unwrap()).unwrap();

        fs::create_dir_all(&paths.models).unwrap();
        fs::write(paths.models.join("orphan.part"), vec![0; 7]).unwrap();
        fs::create_dir(paths.models.join("directory.part")).unwrap();
        fs::write(paths.models.join("ordinary.gguf"), vec![0; 9]).unwrap();

        crate::download::reset_content_hash_count();
        let before_alpha = fs::read_dir(paths.model_dir("alpha").unwrap())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        let inventory = service.incomplete_transfers().unwrap();
        let after_alpha = fs::read_dir(paths.model_dir("alpha").unwrap())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();

        assert_eq!(crate::download::content_hash_count(), 0);
        assert_eq!(before_alpha.len(), after_alpha.len());
        assert_eq!(
            inventory.entries(),
            [
                IncompleteTransferSummary::new("alpha".into(), 31, 100),
                IncompleteTransferSummary::new("zero".into(), 0, 200),
            ]
        );
        assert_eq!(inventory.unrecognized_root_partials(), 1);
        assert!(paths.models.join("orphan.part").exists());
        drop(busy);
    }

    #[test]
    fn inventory_is_empty_when_the_models_root_does_not_exist() {
        let root = tempfile::tempdir().unwrap();
        let (service, _) = service(&root);

        let inventory = service.incomplete_transfers().unwrap();

        assert!(inventory.entries().is_empty());
        assert_eq!(inventory.unrecognized_root_partials(), 0);
    }
}
