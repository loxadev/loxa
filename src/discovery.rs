use crate::huggingface::ResolvedFile;
use reqwest::Url;
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SearchModels {
    query: String,
}

impl SearchModels {
    pub fn new(query: String) -> Self {
        Self { query }
    }

    pub fn query(&self) -> &str {
        &self.query
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModelSearchPage {
    hits: Vec<ModelSearchHit>,
}

impl ModelSearchPage {
    pub fn hits(&self) -> &[ModelSearchHit] {
        &self.hits
    }

    pub(crate) fn new(hits: Vec<ModelSearchHit>) -> Self {
        Self { hits }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModelSearchHit {
    repo: String,
    gated: GatedStatus,
    downloads: Option<u64>,
}

impl ModelSearchHit {
    pub fn repo(&self) -> &str {
        &self.repo
    }

    pub fn gated(&self) -> GatedStatus {
        self.gated
    }

    pub fn downloads(&self) -> Option<u64> {
        self.downloads
    }

    pub(crate) fn new(repo: String, gated: GatedStatus, downloads: Option<u64>) -> Self {
        Self {
            repo,
            gated,
            downloads,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GatedStatus {
    Public,
    AutomaticApproval,
    ManualApproval,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InspectRepository {
    repo: String,
    revision: Option<String>,
}

impl InspectRepository {
    pub fn new(repo: String, revision: Option<String>) -> Self {
        Self { repo, revision }
    }

    pub fn repo(&self) -> &str {
        &self.repo
    }

    pub fn revision(&self) -> Option<&str> {
        self.revision.as_deref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepositoryPlan {
    repo: String,
    commit: String,
    candidates: Vec<ArtifactCandidate>,
}

impl RepositoryPlan {
    pub fn repo(&self) -> &str {
        &self.repo
    }

    pub fn commit(&self) -> &str {
        &self.commit
    }

    pub fn candidates(&self) -> &[ArtifactCandidate] {
        &self.candidates
    }

    pub(crate) fn new(repo: String, commit: String, candidates: Vec<ArtifactCandidate>) -> Self {
        Self {
            repo,
            commit,
            candidates,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactCandidate {
    display_path: String,
    size: Option<u64>,
    identity: Option<ResolvedFile>,
    disposition: CandidateDisposition,
}

impl ArtifactCandidate {
    pub fn display_path(&self) -> &str {
        &self.display_path
    }

    pub fn size(&self) -> Option<u64> {
        self.size
    }

    pub fn identity(&self) -> Option<&ResolvedFile> {
        self.identity.as_ref()
    }

    pub fn disposition(&self) -> CandidateDisposition {
        self.disposition
    }

    pub(crate) fn new(
        display_path: String,
        size: Option<u64>,
        identity: Option<ResolvedFile>,
        disposition: CandidateDisposition,
    ) -> Self {
        Self {
            display_path,
            size,
            identity,
            disposition,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CandidateDisposition {
    EligibleForDownloadAndLocalValidation,
    UnsupportedPackaging(UnsupportedPackagingReason),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnsupportedPackagingReason {
    UnsupportedEntryType,
    UnsafePath,
    NestedPath,
    Sharded,
    Auxiliary(AuxiliaryRole),
    MissingSize,
    ZeroSize,
    MissingLfsIdentity,
    SizeMismatch,
    InvalidLfsSha256,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuxiliaryRole {
    Mtp,
    Draft,
    Mmproj,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DiscoveryError {
    kind: DiscoveryErrorKind,
}

impl DiscoveryError {
    pub fn kind(&self) -> DiscoveryErrorKind {
        self.kind
    }

    pub(crate) const fn new(kind: DiscoveryErrorKind) -> Self {
        Self { kind }
    }
}

impl fmt::Display for DiscoveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Hugging Face discovery request failed")
    }
}

impl std::error::Error for DiscoveryError {}

pub(crate) fn search_models_with(
    request: SearchModels,
    keyword_search: impl FnOnce(&str) -> Result<ModelSearchPage, DiscoveryError>,
) -> Result<ModelSearchPage, DiscoveryError> {
    match route_search_input(request.query())? {
        SearchRoute::Exact(repo) => Ok(ModelSearchPage {
            hits: vec![ModelSearchHit {
                repo,
                gated: GatedStatus::Unknown,
                downloads: None,
            }],
        }),
        SearchRoute::Keyword(query) => keyword_search(&query),
    }
}

enum SearchRoute {
    Exact(String),
    Keyword(String),
}

fn route_search_input(input: &str) -> Result<SearchRoute, DiscoveryError> {
    let input = input.trim();
    if input.contains("://") || input.contains('/') || input.contains('\\') {
        return normalize_search_repository(input).map(SearchRoute::Exact);
    }
    validate_keyword(input).map(SearchRoute::Keyword)
}

fn validate_keyword(input: &str) -> Result<String, DiscoveryError> {
    if !(2..=128).contains(&input.len()) || input.chars().any(char::is_control) {
        return Err(DiscoveryError::new(DiscoveryErrorKind::InvalidQuery));
    }
    Ok(input.into())
}

fn normalize_search_repository(input: &str) -> Result<String, DiscoveryError> {
    if let Some(repo) = input.strip_prefix("hf://") {
        return canonical_repository(repo);
    }
    if input.starts_with("https://") {
        let raw = input
            .strip_prefix("https://")
            .expect("HTTPS prefix was checked");
        let (authority, raw_path) = raw.split_once('/').unwrap_or((raw, ""));
        if authority != "huggingface.co" || raw_path.contains('%') || raw_path.contains('\\') {
            return Err(DiscoveryError::new(DiscoveryErrorKind::InvalidRepository));
        }
        let raw_path = raw_path.strip_suffix('/').unwrap_or(raw_path);
        let repo = canonical_repository(raw_path)?;
        let url = Url::parse(input)
            .map_err(|_| DiscoveryError::new(DiscoveryErrorKind::InvalidRepository))?;
        if url.scheme() != "https"
            || url.host_str() != Some("huggingface.co")
            || url.port().is_some()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(DiscoveryError::new(DiscoveryErrorKind::InvalidRepository));
        }
        return Ok(repo);
    }
    canonical_repository(input)
}

pub(crate) fn validate_repository(input: &str) -> Result<String, DiscoveryError> {
    canonical_repository(input)
}

pub(crate) fn normalize_legacy_pull_repository(input: &str) -> Result<String, DiscoveryError> {
    canonical_repository(input.strip_prefix("hf://").unwrap_or(input))
}

fn canonical_repository(input: &str) -> Result<String, DiscoveryError> {
    let mut components = input.split('/');
    let owner = components.next().unwrap_or_default();
    let repo = components.next().unwrap_or_default();
    if components.next().is_some()
        || input.len() > 193
        || !valid_repository_component(owner)
        || !valid_repository_component(repo)
    {
        return Err(DiscoveryError::new(DiscoveryErrorKind::InvalidRepository));
    }
    Ok(input.into())
}

fn valid_repository_component(component: &str) -> bool {
    let bytes = component.as_bytes();
    (1..=96).contains(&bytes.len())
        && component.is_ascii()
        && matches!(bytes.first(), Some(byte) if byte.is_ascii_alphanumeric() || *byte == b'_')
        && matches!(bytes.last(), Some(byte) if byte.is_ascii_alphanumeric() || *byte == b'_')
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        && !component.contains("--")
        && !component.contains("..")
}

#[cfg(test)]
pub(crate) fn validate_inspect_repository(
    request: &InspectRepository,
) -> Result<String, DiscoveryError> {
    validate_inspect_request(request).map(|(repo, _)| repo)
}

pub(crate) fn validate_inspect_request(
    request: &InspectRepository,
) -> Result<(String, Option<String>), DiscoveryError> {
    let repo = canonical_repository(request.repo())?;
    let revision = request.revision().map(validate_revision).transpose()?;
    Ok((repo, revision))
}

fn validate_revision(revision: &str) -> Result<String, DiscoveryError> {
    if !(1..=256).contains(&revision.len())
        || matches!(revision, "." | "..")
        || revision.chars().any(char::is_control)
    {
        return Err(DiscoveryError::new(DiscoveryErrorKind::InvalidRevision));
    }
    Ok(revision.into())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiscoveryErrorKind {
    InvalidQuery,
    InvalidRepository,
    InvalidRevision,
    AuthenticationRequired,
    AccessDenied,
    RepositoryNotFound,
    RevisionNotFound,
    RateLimited,
    RemoteUnavailable,
    DeadlineExceeded,
    RedirectRejected,
    PaginationRejected,
    ResponseTooLarge,
    MalformedResponse,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct RecordingSearch {
        queries: Vec<String>,
    }

    impl RecordingSearch {
        fn send(&mut self, query: &str) -> Result<ModelSearchPage, DiscoveryError> {
            self.queries.push(query.into());
            Err(DiscoveryError::new(DiscoveryErrorKind::RemoteUnavailable))
        }
    }

    #[test]
    fn discovery_error_exposes_only_a_safe_kind() {
        let error = DiscoveryError::new(DiscoveryErrorKind::InvalidRepository);

        assert_eq!(error.kind(), DiscoveryErrorKind::InvalidRepository);
        assert_eq!(error.to_string(), "Hugging Face discovery request failed");
        assert_eq!(
            format!("{error:?}"),
            "DiscoveryError { kind: InvalidRepository }"
        );
    }

    #[test]
    fn discovery_values_are_send_and_static() {
        fn assert_send_static<T: Send + 'static>() {}

        assert_send_static::<SearchModels>();
        assert_send_static::<ModelSearchPage>();
        assert_send_static::<ModelSearchHit>();
        assert_send_static::<GatedStatus>();
        assert_send_static::<InspectRepository>();
        assert_send_static::<RepositoryPlan>();
        assert_send_static::<ArtifactCandidate>();
        assert_send_static::<CandidateDisposition>();
        assert_send_static::<UnsupportedPackagingReason>();
        assert_send_static::<AuxiliaryRole>();
        assert_send_static::<DiscoveryError>();
        assert_send_static::<DiscoveryErrorKind>();
    }

    #[test]
    fn discovery_request_constructors_are_infallible_and_validation_is_deferred() {
        let search = SearchModels::new("\0".into());
        let inspect = InspectRepository::new("hf://owner/repo".into(), Some("\0".into()));

        assert_eq!(search.query(), "\0");
        assert_eq!(inspect.repo(), "hf://owner/repo");
        assert_eq!(inspect.revision(), Some("\0"));
    }

    #[test]
    fn exact_repository_forms_return_one_unknown_hit_without_transport() {
        let mut transport = RecordingSearch::default();

        for (input, expected_repo) in [
            ("owner/repo", "owner/repo"),
            ("hf://owner/repo", "owner/repo"),
            ("https://huggingface.co/owner/repo/", "owner/repo"),
        ] {
            let page = search_models_with(SearchModels::new(input.into()), |query| {
                transport.send(query)
            })
            .unwrap();

            assert_eq!(page.hits().len(), 1);
            assert_eq!(page.hits()[0].repo(), expected_repo);
            assert_eq!(page.hits()[0].gated(), GatedStatus::Unknown);
            assert_eq!(page.hits()[0].downloads(), None);
        }

        assert!(transport.queries.is_empty());
    }

    #[test]
    fn malformed_exact_intent_fails_locally_without_transport() {
        let mut transport = RecordingSearch::default();

        for input in [
            "owner/",
            "/repo",
            "owner\\repo",
            "https://example.com/owner/repo",
            "https://user@huggingface.co/owner/repo",
            "https://huggingface.co:443/owner/repo",
            "https://huggingface.co/owner/repo?revision=main",
            "https://huggingface.co/owner/repo#fragment",
            "https://huggingface.co/owner/repo%2Fextra",
            "https://huggingface.co/owner/extra/../repo",
            "https://huggingface.co/owner/repo/.",
            "https://huggingface.co/owner/ignored/%2e%2e/repo",
            "https://huggingface.co/owner\\repo",
            "hf://owner",
        ] {
            let error = search_models_with(SearchModels::new(input.into()), |query| {
                transport.send(query)
            })
            .unwrap_err();

            assert_eq!(
                error.kind(),
                DiscoveryErrorKind::InvalidRepository,
                "{input}"
            );
        }

        assert!(transport.queries.is_empty());
    }

    #[test]
    fn repository_identity_enforces_component_grammar_and_bounds() {
        let maximum = format!("{}/{}", "a".repeat(96), "B".repeat(96));
        assert_eq!(maximum.len(), 193);
        assert_eq!(canonical_repository("a/b").unwrap(), "a/b");
        assert_eq!(
            canonical_repository("MiXeD/Name_9.x-y").unwrap(),
            "MiXeD/Name_9.x-y"
        );
        assert_eq!(canonical_repository(&maximum).unwrap(), maximum);

        for invalid in [
            format!("{}/repo", "a".repeat(97)),
            format!("owner/{}", "b".repeat(97)),
            ".owner/repo".into(),
            "owner./repo".into(),
            "-owner/repo".into(),
            "owner-/repo".into(),
            "owner--name/repo".into(),
            "owner..name/repo".into(),
            "owner/repo\0".into(),
            "owner/répo".into(),
        ] {
            assert_eq!(
                canonical_repository(&invalid).unwrap_err().kind(),
                DiscoveryErrorKind::InvalidRepository,
                "{invalid:?}"
            );
        }
    }

    #[test]
    fn keyword_query_trims_and_enforces_byte_and_control_bounds() {
        for (input, expected) in [
            ("ab", "ab"),
            (&"x".repeat(128), &"x".repeat(128)),
            ("  ordinary multiword query  ", "ordinary multiword query"),
        ] {
            match route_search_input(input).unwrap() {
                SearchRoute::Keyword(query) => assert_eq!(query, expected),
                SearchRoute::Exact(repo) => panic!("unexpected exact route for {repo}"),
            }
        }

        let mut transport = RecordingSearch::default();
        for input in [
            "",
            " \t\n ",
            "x",
            &"x".repeat(129),
            "two\0words",
            "two\u{1f}words",
        ] {
            let error = search_models_with(SearchModels::new(input.into()), |query| {
                transport.send(query)
            })
            .unwrap_err();
            assert_eq!(error.kind(), DiscoveryErrorKind::InvalidQuery);
        }
        assert!(transport.queries.is_empty());
    }

    #[test]
    fn inspect_repository_accepts_only_canonical_repository_identity() {
        let request = InspectRepository::new("Owner/Repo".into(), None);
        assert_eq!(validate_inspect_repository(&request).unwrap(), "Owner/Repo");

        for repo in ["hf://Owner/Repo", "https://huggingface.co/Owner/Repo"] {
            let request = InspectRepository::new(repo.into(), None);
            assert_eq!(
                validate_inspect_repository(&request).unwrap_err().kind(),
                DiscoveryErrorKind::InvalidRepository
            );
        }
    }

    #[test]
    fn revision_input_enforces_bounds_and_controls() {
        let one = InspectRepository::new("owner/repo".into(), Some("a".into()));
        assert_eq!(
            validate_inspect_request(&one).unwrap(),
            ("owner/repo".into(), Some("a".into()))
        );
        let maximum = "r".repeat(256);
        let maximum_request = InspectRepository::new("owner/repo".into(), Some(maximum.clone()));
        assert_eq!(
            validate_inspect_request(&maximum_request).unwrap(),
            ("owner/repo".into(), Some(maximum))
        );

        for revision in ["", &"r".repeat(257), "branch\0name", "branch\u{1f}name"] {
            let request = InspectRepository::new("owner/repo".into(), Some(revision.into()));
            assert_eq!(
                validate_inspect_request(&request).unwrap_err().kind(),
                DiscoveryErrorKind::InvalidRevision,
                "{revision:?}"
            );
        }
    }
}
