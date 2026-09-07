mod discovery_public_contract_tests {
    use loxa::app::AppService;
    use loxa::discovery::{
        ArtifactCandidate, AuxiliaryRole, CandidateDisposition, DiscoveryError, DiscoveryErrorKind,
        GatedStatus, InspectRepository, ModelSearchHit, ModelSearchPage, RepositoryPlan,
        SearchModels, UnsupportedPackagingReason,
    };
    use loxa::huggingface::ResolvedFile;

    #[test]
    fn discovery_public_contract_has_exact_owned_accessors() {
        let search = SearchModels::new("two words".into());
        assert_eq!(search.query(), "two words");
        let inspect = InspectRepository::new("owner/repo".into(), Some("main".into()));
        assert_eq!(inspect.repo(), "owner/repo");
        assert_eq!(inspect.revision(), Some("main"));

        let _: fn(&ModelSearchPage) -> &[ModelSearchHit] = ModelSearchPage::hits;
        let _: fn(&ModelSearchHit) -> &str = ModelSearchHit::repo;
        let _: fn(&ModelSearchHit) -> GatedStatus = ModelSearchHit::gated;
        let _: fn(&ModelSearchHit) -> Option<u64> = ModelSearchHit::downloads;
        let _: fn(&RepositoryPlan) -> &str = RepositoryPlan::repo;
        let _: fn(&RepositoryPlan) -> &str = RepositoryPlan::commit;
        let _: fn(&RepositoryPlan) -> &[ArtifactCandidate] = RepositoryPlan::candidates;
        let _: fn(&ArtifactCandidate) -> &str = ArtifactCandidate::display_path;
        let _: fn(&ArtifactCandidate) -> Option<u64> = ArtifactCandidate::size;
        let _: fn(&ArtifactCandidate) -> Option<&ResolvedFile> = ArtifactCandidate::identity;
        let _: fn(&ArtifactCandidate) -> CandidateDisposition = ArtifactCandidate::disposition;
        let _: fn(&DiscoveryError) -> DiscoveryErrorKind = DiscoveryError::kind;
        let _: fn(&AppService, SearchModels) -> Result<ModelSearchPage, DiscoveryError> =
            AppService::search_models;
        let _: fn(&AppService, InspectRepository) -> Result<RepositoryPlan, DiscoveryError> =
            AppService::inspect_repository;

        let _: GatedStatus = GatedStatus::Unknown;
        let _: AuxiliaryRole = AuxiliaryRole::Mtp;
        let _: CandidateDisposition = CandidateDisposition::UnsupportedPackaging(
            UnsupportedPackagingReason::Auxiliary(AuxiliaryRole::Draft),
        );
        let _: DiscoveryErrorKind = DiscoveryErrorKind::RevisionNotFound;
    }
}
mod selected_transfer_public_contract_tests {
    use loxa::app::{
        AppService, DiscardCandidate, ResolveArtifactError, ResolveArtifactRequest,
        TransferControl, TransferDisposition, TransferError, TransferPhase, TransferProgress,
        TransferResult, TransferSelected,
    };
    use loxa::huggingface::ResolvedFile;

    fn assert_send_static<T: Send + 'static>() {}
    fn assert_clone_send_sync_static<T: Clone + Send + Sync + 'static>() {}
    fn assert_error<T: std::error::Error + Send + 'static>() {}

    #[test]
    fn selected_transfer_public_contract_compiles_the_owned_values() {
        assert_send_static::<ResolveArtifactRequest>();
        assert_send_static::<TransferSelected>();
        assert_send_static::<TransferProgress>();
        assert_send_static::<TransferResult>();
        assert_clone_send_sync_static::<TransferControl>();
        assert_error::<ResolveArtifactError>();
        assert_error::<TransferError>();

        let _: fn(String, Option<String>, String) -> ResolveArtifactRequest =
            ResolveArtifactRequest::exact_file;
        let _: fn(String, Option<String>, String) -> ResolveArtifactRequest =
            ResolveArtifactRequest::unique_quant;
        let _: fn(ResolvedFile, Option<String>) -> TransferSelected = TransferSelected::new;
        let _: fn() -> TransferControl = TransferControl::new;
        let _: fn(&TransferControl) = TransferControl::request_pause;
        let _: fn(&TransferProgress) -> TransferPhase = TransferProgress::phase;
        let _: fn(&TransferProgress) -> u64 = TransferProgress::transferred_bytes;
        let _: fn(&TransferProgress) -> u64 = TransferProgress::total_bytes;
        let _: fn(&TransferResult) -> &str = TransferResult::model_id;
        let _: fn(&TransferResult) -> &ResolvedFile = TransferResult::artifact;
        let _: fn(&TransferResult) -> TransferDisposition = TransferResult::disposition;
        let _: fn(&TransferResult) -> Option<u64> = TransferResult::retained_bytes;
        let _: fn(&TransferResult) -> bool = TransferResult::discardable;
        let _: fn(&DiscardCandidate) -> &str = DiscardCandidate::model_id;

        fn service_signatures(
            service: &AppService,
            resolve: ResolveArtifactRequest,
            selected: TransferSelected,
            control: TransferControl,
            candidate: DiscardCandidate,
        ) {
            let _: Result<ResolvedFile, ResolveArtifactError> = service.resolve_artifact(resolve);
            let _: Result<TransferResult, TransferError> =
                service.transfer_selected(selected, control, |_: TransferProgress| {});
            let _: Result<DiscardCandidate, TransferError> =
                service.prepare_discard("model".into());
            let _: Result<(), TransferError> = service.discard_transfer(candidate);
        }
        let _ = service_signatures;

        let _: TransferPhase = TransferPhase::Transferring;
        let _: TransferPhase = TransferPhase::Verifying;
        let _: TransferPhase = TransferPhase::Publishing;
        let _: TransferDisposition = TransferDisposition::Installed;
        let _: TransferDisposition = TransferDisposition::AlreadyInstalled;
        let _: TransferDisposition = TransferDisposition::Paused;
        let _: TransferDisposition = TransferDisposition::Interrupted;
    }
}
