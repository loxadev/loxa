use crate::discovery::{
    self, ArtifactCandidate, AuxiliaryRole, CandidateDisposition, DiscoveryError,
    DiscoveryErrorKind, GatedStatus, InspectRepository, ModelSearchHit, ModelSearchPage,
    RepositoryPlan, SearchModels, UnsupportedPackagingReason,
};
use reqwest::blocking::{Client, RequestBuilder};
use reqwest::header::{HeaderValue, LINK};
use reqwest::{redirect::Policy, StatusCode, Url};
use serde::{de::DeserializeOwned, Deserialize};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::fmt;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const HF_ORIGIN: &str = "https://huggingface.co:443";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const SEARCH_BODY_CAP: usize = 1024 * 1024;
const METADATA_BODY_CAP: usize = 1024 * 1024;
const TREE_BODY_CAP: usize = 2 * 1024 * 1024;
const INSPECTION_DEADLINE: Duration = Duration::from_secs(45);
const TREE_PAGE_LIMIT: usize = 100;
const TREE_PAGE_COUNT_LIMIT: usize = 4;
const TREE_ENTRY_LIMIT: usize = TREE_PAGE_LIMIT * TREE_PAGE_COUNT_LIMIT;
const TREE_CANDIDATE_LIMIT: usize = 256;

trait Clock {
    fn elapsed(&mut self) -> Duration;
}

struct MonotonicClock {
    started: Instant,
}

impl MonotonicClock {
    fn new() -> Self {
        Self {
            started: Instant::now(),
        }
    }
}

impl Clock for MonotonicClock {
    fn elapsed(&mut self) -> Duration {
        self.started.elapsed()
    }
}

struct InspectionDeadline<'clock, C> {
    clock: &'clock mut C,
}

impl<'clock, C: Clock> InspectionDeadline<'clock, C> {
    fn new(clock: &'clock mut C) -> Self {
        Self { clock }
    }

    fn remaining(&mut self) -> Result<Duration, DiscoveryError> {
        let elapsed = self.clock.elapsed();
        if elapsed >= INSPECTION_DEADLINE {
            return Err(DiscoveryError::new(DiscoveryErrorKind::DeadlineExceeded));
        }
        INSPECTION_DEADLINE
            .checked_sub(elapsed)
            .ok_or_else(|| DiscoveryError::new(DiscoveryErrorKind::DeadlineExceeded))
    }

    fn request_timeout(&mut self) -> Result<Duration, DiscoveryError> {
        Ok(REQUEST_TIMEOUT.min(self.remaining()?))
    }

    fn checkpoint(&mut self) -> Result<(), DiscoveryError> {
        self.remaining().map(|_| ())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedFile {
    repo: String,
    revision: String,
    filename: String,
    sha256: String,
    size: u64,
}

impl ResolvedFile {
    pub fn repo(&self) -> &str {
        &self.repo
    }

    pub fn commit(&self) -> &str {
        &self.revision
    }

    pub fn path(&self) -> &str {
        &self.filename
    }

    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    pub fn size(&self) -> u64 {
        self.size
    }
}

#[cfg(test)]
pub(crate) fn test_resolved_file(sha256: String, size: u64) -> ResolvedFile {
    ResolvedFile {
        repo: "owner/repo".into(),
        revision: "0123456789abcdef0123456789abcdef01234567".into(),
        filename: "model.gguf".into(),
        sha256,
        size,
    }
}

#[cfg(any(test, feature = "test-support"))]
fn resolved_file_for_test(repo: &str, path: &str, sha256: String, size: u64) -> ResolvedFile {
    ResolvedFile {
        repo: repo.into(),
        revision: "0123456789abcdef0123456789abcdef01234567".into(),
        filename: path.into(),
        sha256,
        size,
    }
}

#[cfg(all(test, not(feature = "test-support")))]
pub(crate) fn test_resolved_file_for(
    repo: &str,
    path: &str,
    sha256: String,
    size: u64,
) -> ResolvedFile {
    resolved_file_for_test(repo, path, sha256, size)
}

#[cfg(feature = "test-support")]
#[doc(hidden)]
pub fn test_resolved_file_for(repo: &str, path: &str, sha256: String, size: u64) -> ResolvedFile {
    resolved_file_for_test(repo, path, sha256, size)
}

struct TransportRequest {
    url: Url,
    timeout: Duration,
    authorization: bool,
}

struct TransportResponse {
    status: StatusCode,
    content_length: Option<u64>,
    links: Vec<HeaderValue>,
    body: Box<dyn Read>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TransportFailure {
    RemoteUnavailable,
    DeadlineExceeded,
}

trait DiscoveryTransport {
    fn token_available(&self) -> bool {
        false
    }

    fn get(&mut self, request: TransportRequest) -> Result<TransportResponse, TransportFailure>;
}

struct ReqwestDiscoveryTransport {
    client: Client,
    token: Option<String>,
}

impl ReqwestDiscoveryTransport {
    fn new() -> Result<Self, DiscoveryError> {
        let client = Client::builder()
            .user_agent(concat!("loxa/", env!("CARGO_PKG_VERSION")))
            .connect_timeout(REQUEST_TIMEOUT)
            .redirect(Policy::none())
            .build()
            .map_err(|_| DiscoveryError::new(DiscoveryErrorKind::RemoteUnavailable))?;
        Ok(Self {
            client,
            token: discover_token(),
        })
    }

    fn with_client(client: Client, token: Option<String>) -> Self {
        Self { client, token }
    }
}

impl DiscoveryTransport for ReqwestDiscoveryTransport {
    fn token_available(&self) -> bool {
        self.token.is_some()
    }

    fn get(&mut self, request: TransportRequest) -> Result<TransportResponse, TransportFailure> {
        let mut builder = self.client.get(request.url).timeout(request.timeout);
        if request.authorization {
            if let Some(token) = self.token.as_deref() {
                builder = builder.bearer_auth(token);
            }
        }
        let response = builder.send().map_err(classify_discovery_request_error)?;
        let content_length = response.content_length();
        let links = response.headers().get_all(LINK).iter().cloned().collect();
        Ok(TransportResponse {
            status: response.status(),
            content_length,
            links,
            body: Box::new(response),
        })
    }
}

#[derive(Clone, Copy)]
enum DiscoveryEndpoint {
    Search,
    Metadata { requested_revision: bool },
    Tree,
}

pub(crate) fn search_models(request: SearchModels) -> Result<ModelSearchPage, DiscoveryError> {
    let mut transport = None;
    discovery::search_models_with(request, |query| {
        if transport.is_none() {
            transport = Some(ReqwestDiscoveryTransport::new()?);
        }
        keyword_search_with_transport(query, transport.as_mut().expect("transport initialized"))
    })
}

pub(crate) fn inspect_repository(
    request: InspectRepository,
) -> Result<RepositoryPlan, DiscoveryError> {
    let mut transport = ReqwestDiscoveryTransport::new()?;
    inspect_repository_with_transport(request, &mut transport)
}

fn inspect_repository_with_transport<T: DiscoveryTransport>(
    request: InspectRepository,
    transport: &mut T,
) -> Result<RepositoryPlan, DiscoveryError> {
    let mut clock = MonotonicClock::new();
    inspect_repository_with_transport_and_clock(request, transport, &mut clock)
}

fn inspect_repository_with_transport_and_clock<T: DiscoveryTransport, C: Clock>(
    request: InspectRepository,
    transport: &mut T,
    clock: &mut C,
) -> Result<RepositoryPlan, DiscoveryError> {
    let mut deadline = InspectionDeadline::new(clock);
    let (repo, revision) = discovery::validate_inspect_request(&request)?;
    let mut metadata = send_with_deadline(
        transport,
        metadata_url(&repo, revision.as_deref())?,
        &mut deadline,
    )?;
    classify_status(
        DiscoveryEndpoint::Metadata {
            requested_revision: revision.is_some(),
        },
        metadata.status,
    )?;
    let metadata: ModelInfo =
        decode_json(&mut metadata, METADATA_BODY_CAP, || deadline.checkpoint())?;
    let commit = canonical_full_commit(&metadata.sha)?;

    let mut next = Some(root_tree_url(&repo, &commit)?);
    let mut pages = 0usize;
    let mut entries = 0usize;
    let mut candidates = 0usize;
    let mut root_entries = Vec::new();
    let mut seen_urls = BTreeSet::new();
    let mut seen_cursors = BTreeSet::new();
    while let Some(url) = next.take() {
        if !seen_urls.insert(url.as_str().to_owned()) {
            return Err(DiscoveryError::new(DiscoveryErrorKind::PaginationRejected));
        }
        if let Some(cursor) = cursor_from_url(&url) {
            if !seen_cursors.insert(cursor) {
                return Err(DiscoveryError::new(DiscoveryErrorKind::PaginationRejected));
            }
        }
        let mut tree = send_with_deadline(transport, url, &mut deadline)?;
        classify_status(DiscoveryEndpoint::Tree, tree.status)?;
        let links = std::mem::take(&mut tree.links);
        let page: Vec<RootTreeEntry> =
            decode_json(&mut tree, TREE_BODY_CAP, || deadline.checkpoint())?;
        if page.len() > TREE_PAGE_LIMIT {
            return Err(DiscoveryError::new(DiscoveryErrorKind::ResponseTooLarge));
        }
        pages = checked_count(pages, 1, TREE_PAGE_COUNT_LIMIT)?;
        entries = checked_count(entries, page.len(), TREE_ENTRY_LIMIT)?;
        for entry in &page {
            if entry.path.as_deref().is_some_and(is_gguf_like) {
                candidates = checked_count(candidates, 1, TREE_CANDIDATE_LIMIT)?;
            }
        }
        root_entries.extend(page);
        let next_page = next_tree_url(&links, &repo, &commit);
        deadline.checkpoint()?;
        let next_page = next_page?;
        if next_page.is_some() && pages >= TREE_PAGE_COUNT_LIMIT {
            return Err(DiscoveryError::new(DiscoveryErrorKind::ResponseTooLarge));
        }
        next = next_page;
    }
    let candidates = root_entries
        .into_iter()
        .filter(|entry| entry.path.as_deref().is_some_and(is_gguf_like))
        .map(|entry| root_tree_candidate(entry, &repo, &commit))
        .collect();
    deadline.checkpoint()?;
    Ok(RepositoryPlan::new(repo, commit, candidates))
}

fn root_tree_candidate(entry: RootTreeEntry, repo: &str, commit: &str) -> ArtifactCandidate {
    let path = entry.path.as_deref().unwrap_or_default().to_owned();
    let disposition = candidate_disposition(&entry, &path);
    let identity = matches!(
        disposition,
        CandidateDisposition::EligibleForDownloadAndLocalValidation
    )
    .then(|| {
        let size = entry
            .size
            .expect("eligible candidates have a positive size");
        let lfs = entry
            .lfs
            .as_ref()
            .expect("eligible candidates have LFS identity");
        ResolvedFile {
            repo: repo.into(),
            revision: commit.into(),
            filename: path.clone(),
            sha256: lfs
                .oid
                .as_deref()
                .expect("eligible candidates have an LFS OID")
                .to_ascii_lowercase(),
            size,
        }
    });
    ArtifactCandidate::new(safe_display_path(&path), entry.size, identity, disposition)
}

fn candidate_disposition(entry: &RootTreeEntry, path: &str) -> CandidateDisposition {
    let unsupported = |reason| CandidateDisposition::UnsupportedPackaging(reason);
    if entry.kind.as_deref() != Some("file") {
        return unsupported(UnsupportedPackagingReason::UnsupportedEntryType);
    }
    if unsafe_root_path(path) {
        return unsupported(UnsupportedPackagingReason::UnsafePath);
    }
    if path.contains('/') {
        return unsupported(UnsupportedPackagingReason::NestedPath);
    }
    if numeric_shard_marker(path) {
        return unsupported(UnsupportedPackagingReason::Sharded);
    }
    if let Some(role) = crate::catalog::local::auxiliary_role(path) {
        let role = match role {
            crate::catalog::local::AuxiliaryRole::Mtp => AuxiliaryRole::Mtp,
            crate::catalog::local::AuxiliaryRole::Draft => AuxiliaryRole::Draft,
            crate::catalog::local::AuxiliaryRole::Mmproj => AuxiliaryRole::Mmproj,
        };
        return unsupported(UnsupportedPackagingReason::Auxiliary(role));
    }
    let Some(size) = entry.size else {
        return unsupported(UnsupportedPackagingReason::MissingSize);
    };
    if size == 0 {
        return unsupported(UnsupportedPackagingReason::ZeroSize);
    }
    let Some(lfs) = entry.lfs.as_ref() else {
        return unsupported(UnsupportedPackagingReason::MissingLfsIdentity);
    };
    let (Some(oid), Some(lfs_size)) = (lfs.oid.as_deref(), lfs.size) else {
        return unsupported(UnsupportedPackagingReason::MissingLfsIdentity);
    };
    if lfs_size != size {
        return unsupported(UnsupportedPackagingReason::SizeMismatch);
    }
    if oid.len() != 64 || !oid.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return unsupported(UnsupportedPackagingReason::InvalidLfsSha256);
    }
    CandidateDisposition::EligibleForDownloadAndLocalValidation
}

fn unsafe_root_path(path: &str) -> bool {
    path.is_empty()
        || path.contains('\\')
        || path.starts_with('/')
        || path.chars().any(unsafe_presentation_character)
        || path.split('/').any(|segment| matches!(segment, "." | ".."))
}

fn safe_display_path(path: &str) -> String {
    if path.is_empty() || path.chars().any(unsafe_presentation_character) {
        "<unsafe path>".into()
    } else {
        path.into()
    }
}

pub(crate) fn unsafe_presentation_character(character: char) -> bool {
    character.is_control()
        || matches!(
            character,
            '\u{061c}'
                | '\u{200e}'
                | '\u{200f}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202a}'..='\u{202e}'
                | '\u{2066}'..='\u{2069}'
        )
}

fn numeric_shard_marker(path: &str) -> bool {
    let bytes = path.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if !bytes[index].is_ascii_digit() {
            index += 1;
            continue;
        }
        let first_end = bytes[index..]
            .iter()
            .position(|byte| !byte.is_ascii_digit())
            .map(|offset| index + offset)
            .unwrap_or(bytes.len());
        let marker = first_end;
        if bytes.get(marker..marker + 4) != Some(b"-of-") {
            index = first_end;
            continue;
        }
        let second_start = marker + 4;
        let second_end = bytes[second_start..]
            .iter()
            .position(|byte| !byte.is_ascii_digit())
            .map(|offset| second_start + offset)
            .unwrap_or(bytes.len());
        if second_start != second_end {
            return true;
        }
        index = second_end.max(second_start);
    }
    false
}

fn checked_count(current: usize, add: usize, limit: usize) -> Result<usize, DiscoveryError> {
    let next = current
        .checked_add(add)
        .ok_or_else(|| DiscoveryError::new(DiscoveryErrorKind::ResponseTooLarge))?;
    if next > limit {
        return Err(DiscoveryError::new(DiscoveryErrorKind::ResponseTooLarge));
    }
    Ok(next)
}

fn is_gguf_like(path: &str) -> bool {
    path.len() >= 5
        && path
            .get(path.len() - 5..)
            .is_some_and(|suffix| suffix.eq_ignore_ascii_case(".gguf"))
}

fn cursor_from_url(url: &Url) -> Option<String> {
    url.query_pairs()
        .find_map(|(key, value)| (key == "cursor").then(|| value.into_owned()))
}

fn next_tree_url(
    links: &[HeaderValue],
    repo: &str,
    commit: &str,
) -> Result<Option<Url>, DiscoveryError> {
    let mut next = None;
    for value in links {
        let value = value
            .to_str()
            .map_err(|_| DiscoveryError::new(DiscoveryErrorKind::PaginationRejected))?;
        for link in parse_link_header(value)? {
            if link.relations.iter().any(|relation| relation == "next") {
                if next.is_some() {
                    return Err(DiscoveryError::new(DiscoveryErrorKind::PaginationRejected));
                }
                next = Some(link.target);
            }
        }
    }
    next.map(|target| validate_next_tree_url(&target, repo, commit))
        .transpose()
}

struct ParsedLink {
    target: String,
    relations: Vec<String>,
}

fn parse_link_header(value: &str) -> Result<Vec<ParsedLink>, DiscoveryError> {
    let bytes = value.as_bytes();
    let mut index = 0;
    let mut links = Vec::new();
    loop {
        skip_ows(bytes, &mut index);
        if index >= bytes.len() || bytes[index] != b'<' {
            return Err(DiscoveryError::new(DiscoveryErrorKind::PaginationRejected));
        }
        index += 1;
        let target_start = index;
        while index < bytes.len() && bytes[index] != b'>' {
            if bytes[index].is_ascii_control() {
                return Err(DiscoveryError::new(DiscoveryErrorKind::PaginationRejected));
            }
            index += 1;
        }
        if index == target_start || index >= bytes.len() {
            return Err(DiscoveryError::new(DiscoveryErrorKind::PaginationRejected));
        }
        let target = value[target_start..index].to_owned();
        validate_link_target_syntax(&target)?;
        index += 1;
        let mut relations = Vec::new();
        let mut saw_relation = false;

        loop {
            skip_ows(bytes, &mut index);
            if index == bytes.len() {
                if !saw_relation || relations.is_empty() {
                    return Err(DiscoveryError::new(DiscoveryErrorKind::PaginationRejected));
                }
                links.push(ParsedLink { target, relations });
                return Ok(links);
            }
            if bytes[index] == b',' {
                index += 1;
                if !saw_relation || relations.is_empty() {
                    return Err(DiscoveryError::new(DiscoveryErrorKind::PaginationRejected));
                }
                links.push(ParsedLink { target, relations });
                break;
            }
            if bytes[index] != b';' {
                return Err(DiscoveryError::new(DiscoveryErrorKind::PaginationRejected));
            }
            index += 1;
            skip_ows(bytes, &mut index);
            let name_start = index;
            while index < bytes.len() && is_http_token(bytes[index]) {
                index += 1;
            }
            if name_start == index {
                return Err(DiscoveryError::new(DiscoveryErrorKind::PaginationRejected));
            }
            let name = &value[name_start..index];
            skip_ows(bytes, &mut index);
            if index >= bytes.len() || bytes[index] != b'=' {
                return Err(DiscoveryError::new(DiscoveryErrorKind::PaginationRejected));
            }
            index += 1;
            skip_ows(bytes, &mut index);
            let parameter = parse_link_parameter(value, &mut index)?;
            if name.eq_ignore_ascii_case("rel") {
                if saw_relation {
                    return Err(DiscoveryError::new(DiscoveryErrorKind::PaginationRejected));
                }
                saw_relation = true;
                if parameter
                    .as_bytes()
                    .first()
                    .is_some_and(u8::is_ascii_whitespace)
                    || parameter
                        .as_bytes()
                        .last()
                        .is_some_and(u8::is_ascii_whitespace)
                {
                    return Err(DiscoveryError::new(DiscoveryErrorKind::PaginationRejected));
                }
                for relation in parameter.split_ascii_whitespace() {
                    if !relation.as_bytes().iter().copied().all(is_http_token) {
                        return Err(DiscoveryError::new(DiscoveryErrorKind::PaginationRejected));
                    }
                    relations.push(relation.to_ascii_lowercase());
                }
                if relations.is_empty() {
                    return Err(DiscoveryError::new(DiscoveryErrorKind::PaginationRejected));
                }
            }
        }
    }
}

fn parse_link_parameter(value: &str, index: &mut usize) -> Result<String, DiscoveryError> {
    let bytes = value.as_bytes();
    if *index >= bytes.len() {
        return Err(DiscoveryError::new(DiscoveryErrorKind::PaginationRejected));
    }
    if bytes[*index] != b'"' {
        let start = *index;
        while *index < bytes.len() && is_http_token(bytes[*index]) {
            *index += 1;
        }
        if start == *index {
            return Err(DiscoveryError::new(DiscoveryErrorKind::PaginationRejected));
        }
        return Ok(value[start..*index].to_owned());
    }

    *index += 1;
    let mut decoded = String::new();
    let mut escaped = false;
    while *index < bytes.len() {
        let byte = bytes[*index];
        *index += 1;
        if byte.is_ascii_control() {
            return Err(DiscoveryError::new(DiscoveryErrorKind::PaginationRejected));
        }
        if escaped {
            decoded.push(byte as char);
            escaped = false;
        } else if byte == b'\\' {
            escaped = true;
        } else if byte == b'"' {
            return Ok(decoded);
        } else {
            decoded.push(byte as char);
        }
    }
    Err(DiscoveryError::new(DiscoveryErrorKind::PaginationRejected))
}

fn validate_link_target_syntax(target: &str) -> Result<(), DiscoveryError> {
    validate_raw_uri_reference(target)?;
    let origin = origin_url()?;
    Url::options()
        .base_url(Some(&origin))
        .parse(target)
        .map(|_| ())
        .map_err(|_| DiscoveryError::new(DiscoveryErrorKind::PaginationRejected))
}

fn validate_raw_uri_reference(target: &str) -> Result<(), DiscoveryError> {
    let bytes = target.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte == b'%' {
            if !bytes
                .get(index + 1)
                .copied()
                .is_some_and(|byte| byte.is_ascii_hexdigit())
                || !bytes
                    .get(index + 2)
                    .copied()
                    .is_some_and(|byte| byte.is_ascii_hexdigit())
            {
                return Err(DiscoveryError::new(DiscoveryErrorKind::PaginationRejected));
            }
            index += 3;
        } else if byte.is_ascii_alphanumeric() || b"-._~:/?#[]@!$&'()*+,;=".contains(&byte) {
            index += 1;
        } else {
            return Err(DiscoveryError::new(DiscoveryErrorKind::PaginationRejected));
        }
    }
    Ok(())
}

fn skip_ows(bytes: &[u8], index: &mut usize) {
    while *index < bytes.len() && matches!(bytes[*index], b' ' | b'\t') {
        *index += 1;
    }
}

fn is_http_token(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte)
}

fn validate_next_tree_url(target: &str, repo: &str, commit: &str) -> Result<Url, DiscoveryError> {
    let expected = root_tree_url(repo, commit)?;
    validate_raw_next_tree_path(target, expected.path())?;
    let url = Url::parse(target)
        .map_err(|_| DiscoveryError::new(DiscoveryErrorKind::PaginationRejected))?;
    if url.scheme() != "https"
        || url.host_str() != Some("huggingface.co")
        || url.port_or_known_default() != Some(443)
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || url.path() != expected.path()
    {
        return Err(DiscoveryError::new(DiscoveryErrorKind::PaginationRejected));
    }

    let mut values = BTreeMap::new();
    for (key, value) in url.query_pairs() {
        let key = key.into_owned();
        let value = value.into_owned();
        if values.insert(key, value).is_some() {
            return Err(DiscoveryError::new(DiscoveryErrorKind::PaginationRejected));
        }
    }
    let tree_page_limit = TREE_PAGE_LIMIT.to_string();
    let static_values = [
        ("recursive", "false"),
        ("expand", "true"),
        ("limit", tree_page_limit.as_str()),
    ];
    if values.len() != 4
        || static_values
            .iter()
            .any(|(key, value)| values.get(*key).map(String::as_str) != Some(*value))
        || values
            .get("cursor")
            .is_none_or(|cursor| cursor.is_empty() || cursor.chars().any(char::is_control))
    {
        return Err(DiscoveryError::new(DiscoveryErrorKind::PaginationRejected));
    }
    Ok(url)
}

fn validate_raw_next_tree_path(target: &str, expected_path: &str) -> Result<(), DiscoveryError> {
    let raw = target
        .strip_prefix("https://huggingface.co")
        .ok_or_else(|| DiscoveryError::new(DiscoveryErrorKind::PaginationRejected))?;
    let path_end = raw.find(['?', '#']).unwrap_or(raw.len());
    let raw_path = &raw[..path_end];
    if raw_path != expected_path || raw_path.contains('%') || raw_path.contains('\\') {
        return Err(DiscoveryError::new(DiscoveryErrorKind::PaginationRejected));
    }
    Ok(())
}

fn metadata_url(repo: &str, revision: Option<&str>) -> Result<Url, DiscoveryError> {
    let (owner, name) = repo
        .split_once('/')
        .ok_or_else(|| DiscoveryError::new(DiscoveryErrorKind::InvalidRepository))?;
    let mut url = origin_url()?;
    let mut path = url
        .path_segments_mut()
        .map_err(|_| DiscoveryError::new(DiscoveryErrorKind::RemoteUnavailable))?;
    path.extend(["api", "models", owner, name]);
    if let Some(revision) = revision {
        path.extend(["revision", revision]);
    }
    drop(path);
    Ok(url)
}

fn root_tree_url(repo: &str, commit: &str) -> Result<Url, DiscoveryError> {
    let (owner, name) = repo
        .split_once('/')
        .ok_or_else(|| DiscoveryError::new(DiscoveryErrorKind::InvalidRepository))?;
    let mut url = origin_url()?;
    url.path_segments_mut()
        .map_err(|_| DiscoveryError::new(DiscoveryErrorKind::RemoteUnavailable))?
        .extend(["api", "models", owner, name, "tree", commit]);
    let tree_page_limit = TREE_PAGE_LIMIT.to_string();
    url.query_pairs_mut()
        .append_pair("recursive", "false")
        .append_pair("expand", "true")
        .append_pair("limit", &tree_page_limit);
    Ok(url)
}

fn canonical_full_commit(value: &str) -> Result<String, DiscoveryError> {
    if value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(value.to_ascii_lowercase())
    } else {
        Err(DiscoveryError::new(DiscoveryErrorKind::MalformedResponse))
    }
}

#[cfg(test)]
fn search_models_with_transport<T: DiscoveryTransport>(
    request: SearchModels,
    transport: &mut T,
) -> Result<ModelSearchPage, DiscoveryError> {
    discovery::search_models_with(request, |query| {
        keyword_search_with_transport(query, transport)
    })
}

fn keyword_search_with_transport<T: DiscoveryTransport>(
    query: &str,
    transport: &mut T,
) -> Result<ModelSearchPage, DiscoveryError> {
    let url = search_url(query)?;
    let mut response = send(transport, url, REQUEST_TIMEOUT)?;
    classify_status(DiscoveryEndpoint::Search, response.status)?;
    let entries: Vec<SearchEntry> = decode_json(&mut response, SEARCH_BODY_CAP, || Ok(()))?;
    if entries.len() > 20 {
        return Err(DiscoveryError::new(DiscoveryErrorKind::ResponseTooLarge));
    }
    let mut unique = BTreeMap::<String, (GatedStatus, Option<u64>)>::new();
    for entry in entries {
        let repo = discovery::validate_repository(&entry.id)
            .map_err(|_| DiscoveryError::new(DiscoveryErrorKind::MalformedResponse))?;
        let metadata = (gated_status(&entry.gated), entry.downloads);
        match unique.get(&repo) {
            Some(existing) if existing != &metadata => {
                return Err(DiscoveryError::new(DiscoveryErrorKind::MalformedResponse));
            }
            Some(_) => {}
            None => {
                unique.insert(repo, metadata);
            }
        }
    }
    let mut hits = unique
        .into_iter()
        .map(|(repo, (gated, downloads))| ModelSearchHit::new(repo, gated, downloads))
        .collect::<Vec<_>>();
    hits.sort_by(|left, right| {
        match (left.downloads(), right.downloads()) {
            (Some(left), Some(right)) => right.cmp(&left),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        }
        .then_with(|| left.repo().as_bytes().cmp(right.repo().as_bytes()))
    });
    Ok(ModelSearchPage::new(hits))
}

fn search_url(query: &str) -> Result<Url, DiscoveryError> {
    let mut url = origin_url()?;
    url.path_segments_mut()
        .map_err(|_| DiscoveryError::new(DiscoveryErrorKind::RemoteUnavailable))?
        .extend(["api", "models"]);
    url.query_pairs_mut()
        .append_pair("search", query)
        .append_pair("filter", "gguf")
        .append_pair("sort", "downloads")
        .append_pair("direction", "-1")
        .append_pair("limit", "20");
    Ok(url)
}

fn origin_url() -> Result<Url, DiscoveryError> {
    Url::parse(HF_ORIGIN).map_err(|_| DiscoveryError::new(DiscoveryErrorKind::RemoteUnavailable))
}

fn send<T: DiscoveryTransport>(
    transport: &mut T,
    url: Url,
    timeout: Duration,
) -> Result<TransportResponse, DiscoveryError> {
    let authorization = transport.token_available() && should_attach_token(&url);
    transport
        .get(TransportRequest {
            url,
            timeout,
            authorization,
        })
        .map_err(|failure| match failure {
            TransportFailure::RemoteUnavailable => {
                DiscoveryError::new(DiscoveryErrorKind::RemoteUnavailable)
            }
            TransportFailure::DeadlineExceeded => {
                DiscoveryError::new(DiscoveryErrorKind::DeadlineExceeded)
            }
        })
}

fn send_with_deadline<T: DiscoveryTransport, C: Clock>(
    transport: &mut T,
    url: Url,
    deadline: &mut InspectionDeadline<'_, C>,
) -> Result<TransportResponse, DiscoveryError> {
    let timeout = deadline.request_timeout()?;
    let response = send(transport, url, timeout)?;
    deadline.checkpoint()?;
    Ok(response)
}

fn classify_status(endpoint: DiscoveryEndpoint, status: StatusCode) -> Result<(), DiscoveryError> {
    if status.is_success() {
        return Ok(());
    }
    let kind = if status.is_redirection() {
        DiscoveryErrorKind::RedirectRejected
    } else {
        match status {
            StatusCode::UNAUTHORIZED => DiscoveryErrorKind::AuthenticationRequired,
            StatusCode::FORBIDDEN => DiscoveryErrorKind::AccessDenied,
            StatusCode::TOO_MANY_REQUESTS => DiscoveryErrorKind::RateLimited,
            StatusCode::NOT_FOUND => match endpoint {
                DiscoveryEndpoint::Metadata {
                    requested_revision: true,
                } => DiscoveryErrorKind::RevisionNotFound,
                DiscoveryEndpoint::Metadata { .. } | DiscoveryEndpoint::Tree => {
                    DiscoveryErrorKind::RepositoryNotFound
                }
                DiscoveryEndpoint::Search => DiscoveryErrorKind::RemoteUnavailable,
            },
            _ => DiscoveryErrorKind::RemoteUnavailable,
        }
    };
    Err(DiscoveryError::new(kind))
}

fn decode_json<T: DeserializeOwned>(
    response: &mut TransportResponse,
    cap: usize,
    mut checkpoint: impl FnMut() -> Result<(), DiscoveryError>,
) -> Result<T, DiscoveryError> {
    if response
        .content_length
        .is_some_and(|length| length > cap as u64)
    {
        return Err(DiscoveryError::new(DiscoveryErrorKind::ResponseTooLarge));
    }
    let limit = cap
        .checked_add(1)
        .ok_or_else(|| DiscoveryError::new(DiscoveryErrorKind::ResponseTooLarge))?;
    let mut bytes = Vec::with_capacity(cap.min(16 * 1024));
    let mut body = response.body.by_ref().take(limit as u64);
    let read = body.read_to_end(&mut bytes);
    checkpoint()?;
    read.map_err(|error| {
        let kind = if is_timeout_body_read(&error) {
            DiscoveryErrorKind::DeadlineExceeded
        } else {
            DiscoveryErrorKind::RemoteUnavailable
        };
        DiscoveryError::new(kind)
    })?;
    if bytes.len() > cap {
        return Err(DiscoveryError::new(DiscoveryErrorKind::ResponseTooLarge));
    }
    let decoded = serde_json::from_slice(&bytes);
    checkpoint()?;
    decoded.map_err(|_| DiscoveryError::new(DiscoveryErrorKind::MalformedResponse))
}

fn is_timeout_body_read(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::TimedOut
        || error
            .get_ref()
            .and_then(|source| source.downcast_ref::<reqwest::Error>())
            .is_some_and(|source| source.is_timeout())
}

fn classify_discovery_request_error(error: reqwest::Error) -> TransportFailure {
    if error.is_timeout() {
        TransportFailure::DeadlineExceeded
    } else {
        TransportFailure::RemoteUnavailable
    }
}

#[derive(Deserialize)]
struct SearchEntry {
    id: String,
    #[serde(default)]
    gated: serde_json::Value,
    #[serde(default)]
    downloads: Option<u64>,
}

fn gated_status(value: &serde_json::Value) -> GatedStatus {
    match value {
        serde_json::Value::Bool(false) => GatedStatus::Public,
        serde_json::Value::String(value) if value == "auto" => GatedStatus::AutomaticApproval,
        serde_json::Value::String(value) if value == "manual" => GatedStatus::ManualApproval,
        _ => GatedStatus::Unknown,
    }
}

#[derive(Deserialize)]
struct ModelInfo {
    sha: String,
}

#[derive(Deserialize)]
struct RootTreeEntry {
    #[serde(rename = "type")]
    kind: Option<String>,
    path: Option<String>,
    size: Option<u64>,
    lfs: Option<RootTreeLfs>,
}

#[derive(Deserialize)]
struct RootTreeLfs {
    oid: Option<String>,
    size: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SelectionError {
    MissingSelection,
    NoEligibleFiles,
    FileNotFound(String),
    QuantUnavailable {
        requested: String,
        available: Vec<String>,
    },
    AmbiguousQuant {
        requested: String,
        filenames: Vec<String>,
    },
}

impl fmt::Display for SelectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingSelection => formatter.write_str("GGUF selection requires --file or --quant"),
            Self::NoEligibleFiles => {
                formatter.write_str("repository has no verified single-file GGUF")
            }
            Self::FileNotFound(filename) => {
                write!(formatter, "verified file {filename:?} not found")
            }
            Self::QuantUnavailable {
                requested,
                available,
            } => write!(
                formatter,
                "quantization {requested:?} is not available; available quantizations: {}. Retry with --quant <one of these values>.",
                available.join(", ")
            ),
            Self::AmbiguousQuant {
                requested,
                filenames,
            } => write!(
                formatter,
                "quantization {requested:?} matched multiple files: {}. Use --file <filename> to choose one.",
                filenames.join(", ")
            ),
        }
    }
}

impl std::error::Error for SelectionError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ResolveError {
    Discovery(DiscoveryError),
    Selection(SelectionError),
}

impl fmt::Display for ResolveError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Discovery(error) => error.fmt(formatter),
            Self::Selection(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for ResolveError {}

pub(crate) fn resolve(
    client: &Client,
    repo: &str,
    revision: Option<&str>,
    filename: Option<&str>,
    quant: Option<&str>,
    token: Option<&str>,
) -> Result<ResolvedFile, ResolveError> {
    let request = InspectRepository::new(repo.into(), revision.map(str::to_owned));
    let mut transport =
        ReqwestDiscoveryTransport::with_client(client.clone(), token.map(str::to_owned));
    resolve_with_transport(request, filename, quant, &mut transport)
}

pub(crate) fn resolve_selected(
    repo: &str,
    revision: Option<&str>,
    filename: Option<&str>,
    quant: Option<&str>,
) -> Result<ResolvedFile, ResolveError> {
    let client = Client::builder()
        .user_agent(concat!("loxa/", env!("CARGO_PKG_VERSION")))
        .connect_timeout(Duration::from_secs(30))
        .timeout(Duration::from_secs(30))
        .redirect(Policy::none())
        .build()
        .map_err(|_| {
            ResolveError::Discovery(DiscoveryError::new(DiscoveryErrorKind::RemoteUnavailable))
        })?;
    let token = discover_token();
    resolve(&client, repo, revision, filename, quant, token.as_deref())
}

fn resolve_with_transport<T: DiscoveryTransport>(
    request: InspectRepository,
    filename: Option<&str>,
    quant: Option<&str>,
    transport: &mut T,
) -> Result<ResolvedFile, ResolveError> {
    let plan =
        inspect_repository_with_transport(request, transport).map_err(ResolveError::Discovery)?;
    select_from_plan(&plan, filename, quant).map_err(ResolveError::Selection)
}

fn select_from_plan(
    plan: &RepositoryPlan,
    filename: Option<&str>,
    quant: Option<&str>,
) -> Result<ResolvedFile, SelectionError> {
    if filename.is_none() && quant.is_none() {
        return Err(SelectionError::MissingSelection);
    }
    let mut candidates = plan
        .candidates()
        .iter()
        .filter_map(|candidate| candidate.identity().cloned())
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Err(SelectionError::NoEligibleFiles);
    }
    if let Some(filename) = filename {
        candidates.retain(|candidate| candidate.path() == filename);
        if candidates.len() != 1 {
            return Err(SelectionError::FileNotFound(filename.into()));
        }
    } else if let Some(quant) = quant {
        let matching = candidates
            .iter()
            .filter(|candidate| quantization(candidate.path()).eq_ignore_ascii_case(quant))
            .collect::<Vec<_>>();
        match matching.len() {
            0 => {
                let mut available = candidates
                    .iter()
                    .map(|candidate| quantization(candidate.path()).to_owned())
                    .collect::<Vec<_>>();
                available.sort_unstable();
                available.dedup();
                return Err(SelectionError::QuantUnavailable {
                    requested: quant.into(),
                    available,
                });
            }
            1 => candidates
                .retain(|candidate| quantization(candidate.path()).eq_ignore_ascii_case(quant)),
            _ => {
                let mut filenames = matching
                    .into_iter()
                    .map(|candidate| candidate.path().to_owned())
                    .collect::<Vec<_>>();
                filenames.sort_unstable();
                return Err(SelectionError::AmbiguousQuant {
                    requested: quant.into(),
                    filenames,
                });
            }
        }
    }
    Ok(candidates.remove(0))
}

#[derive(Clone, Debug, Default)]
struct EnvironmentInput {
    disable_implicit_token: Option<OsString>,
    token: Option<OsString>,
    token_path: Option<OsString>,
    hf_home: Option<OsString>,
    home: Option<OsString>,
}

impl EnvironmentInput {
    fn current() -> Self {
        Self {
            disable_implicit_token: std::env::var_os("HF_HUB_DISABLE_IMPLICIT_TOKEN"),
            token: std::env::var_os("HF_TOKEN"),
            token_path: std::env::var_os("HF_TOKEN_PATH"),
            hf_home: std::env::var_os("HF_HOME"),
            home: std::env::var_os("HOME"),
        }
    }
}

pub fn discover_token() -> Option<String> {
    discover_token_from(&EnvironmentInput::current())
}

fn discover_token_from(environment: &EnvironmentInput) -> Option<String> {
    if environment
        .disable_implicit_token
        .as_ref()
        .is_some_and(|value| !value.is_empty())
    {
        return None;
    }
    if let Some(token) = environment.token.as_ref().and_then(|token| token.to_str()) {
        let token = token.trim();
        if !token.is_empty() {
            return Some(token.into());
        }
    }
    if let Some(path) = environment.token_path.as_ref().map(PathBuf::from) {
        return read_token(&path);
    }
    let home = environment.hf_home.as_ref().map(PathBuf::from).or_else(|| {
        environment
            .home
            .as_ref()
            .map(|home| PathBuf::from(home).join(".cache/huggingface"))
    });
    home.and_then(|home| read_token(&home.join("token")))
}

pub fn authorized_request(client: &Client, url: Url, token: Option<&str>) -> RequestBuilder {
    let attach = should_attach_token(&url);
    let request = client.get(url);
    match (attach, token) {
        (true, Some(token)) => request.bearer_auth(token),
        _ => request,
    }
}

pub(crate) fn should_attach_token(url: &Url) -> bool {
    url.scheme() == "https"
        && url.host_str() == Some("huggingface.co")
        && url.port_or_known_default() == Some(443)
}

fn read_token(path: &Path) -> Option<String> {
    let value = fs::read_to_string(path).ok()?;
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}

fn quantization(filename: &str) -> &str {
    filename
        .rsplit_once('.')
        .filter(|(_, extension)| extension.eq_ignore_ascii_case("gguf"))
        .map_or(filename, |(stem, _)| stem)
        .rsplit(['-', '.'])
        .next()
        .unwrap_or("")
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::AUTHORIZATION;
    use std::cell::Cell;
    use std::collections::VecDeque;
    use std::process::Command;
    use std::rc::Rc;

    const MODEL: &str = r#"{"sha":"0123456789abcdef0123456789abcdef01234567","gated":true}"#;
    const TREE: &str = r#"[
      {"type":"file","path":"demo-Q4_K_M.gguf","size":4,"lfs":{"oid":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","size":4}},
      {"type":"file","path":"demo-Q8_0.gguf","size":8,"lfs":{"oid":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","size":8}},
      {"type":"file","path":"split-00001-of-00002.gguf","size":2,"lfs":{"oid":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","size":2}}
    ]"#;

    fn inspected_plan(tree: &str) -> RepositoryPlan {
        let mut transport = FakeDiscoveryTransport::queued(vec![
            FakeDiscoveryTransport::json_response(StatusCode::OK, MODEL),
            FakeDiscoveryTransport::json_response(StatusCode::OK, tree),
        ]);
        inspect_repository_with_transport(
            InspectRepository::new("owner/repo".into(), None),
            &mut transport,
        )
        .unwrap()
    }

    struct FakeDiscoveryTransport {
        responses: VecDeque<Result<TransportResponse, TransportFailure>>,
        requests: Vec<TransportRequest>,
        token_available: bool,
    }

    impl FakeDiscoveryTransport {
        fn json(status: StatusCode, body: &str) -> Self {
            Self::response(
                status,
                Some(body.len() as u64),
                vec![body.as_bytes().to_vec()],
            )
            .0
        }

        fn response(
            status: StatusCode,
            content_length: Option<u64>,
            chunks: Vec<Vec<u8>>,
        ) -> (Self, Rc<Cell<usize>>) {
            let reads = Rc::new(Cell::new(0));
            let transport = Self {
                responses: VecDeque::from([Ok(TransportResponse {
                    status,
                    content_length,
                    links: Vec::new(),
                    body: Box::new(ChunkReader {
                        chunks: chunks.into(),
                        reads: reads.clone(),
                    }),
                })]),
                requests: Vec::new(),
                token_available: false,
            };
            (transport, reads)
        }

        fn response_with_counter(
            status: StatusCode,
            content_length: Option<u64>,
            chunks: Vec<Vec<u8>>,
            links: Vec<HeaderValue>,
        ) -> (TransportResponse, Rc<Cell<usize>>) {
            let reads = Rc::new(Cell::new(0));
            let response = TransportResponse {
                status,
                content_length,
                links,
                body: Box::new(ChunkReader {
                    chunks: chunks.into(),
                    reads: reads.clone(),
                }),
            };
            (response, reads)
        }

        fn failure(failure: TransportFailure) -> Self {
            Self {
                responses: VecDeque::from([Err(failure)]),
                requests: Vec::new(),
                token_available: false,
            }
        }

        fn queued(responses: Vec<TransportResponse>) -> Self {
            Self {
                responses: responses.into_iter().map(Ok).collect(),
                requests: Vec::new(),
                token_available: false,
            }
        }

        fn json_response(status: StatusCode, body: &str) -> TransportResponse {
            Self::json_response_with_links(status, body, Vec::new())
        }

        fn json_response_without_length(status: StatusCode, body: &str) -> TransportResponse {
            TransportResponse {
                status,
                content_length: None,
                links: Vec::new(),
                body: Box::new(ChunkReader {
                    chunks: VecDeque::from([body.as_bytes().to_vec()]),
                    reads: Rc::new(Cell::new(0)),
                }),
            }
        }

        fn json_response_with_links(
            status: StatusCode,
            body: &str,
            links: Vec<HeaderValue>,
        ) -> TransportResponse {
            TransportResponse {
                status,
                content_length: Some(body.len() as u64),
                links,
                body: Box::new(ChunkReader {
                    chunks: VecDeque::from([body.as_bytes().to_vec()]),
                    reads: Rc::new(Cell::new(0)),
                }),
            }
        }
    }

    struct ChunkReader {
        chunks: VecDeque<Vec<u8>>,
        reads: Rc<Cell<usize>>,
    }

    impl std::io::Read for ChunkReader {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            self.reads.set(self.reads.get() + 1);
            while let Some(chunk) = self.chunks.front_mut() {
                if chunk.is_empty() {
                    self.chunks.pop_front();
                    continue;
                }
                let length = buffer.len().min(chunk.len());
                buffer[..length].copy_from_slice(&chunk[..length]);
                chunk.drain(..length);
                return Ok(length);
            }
            Ok(0)
        }
    }

    struct FakeClock {
        readings: VecDeque<Duration>,
        fallback: Duration,
    }

    impl FakeClock {
        fn new(readings: impl IntoIterator<Item = Duration>) -> Self {
            let readings = readings.into_iter().collect::<VecDeque<_>>();
            let fallback = readings.back().copied().unwrap_or_default();
            Self { readings, fallback }
        }
    }

    impl Clock for FakeClock {
        fn elapsed(&mut self) -> Duration {
            self.readings.pop_front().unwrap_or(self.fallback)
        }
    }

    impl DiscoveryTransport for FakeDiscoveryTransport {
        fn token_available(&self) -> bool {
            self.token_available
        }

        fn get(
            &mut self,
            request: TransportRequest,
        ) -> Result<TransportResponse, TransportFailure> {
            self.requests.push(request);
            self.responses
                .pop_front()
                .unwrap_or(Err(TransportFailure::RemoteUnavailable))
        }
    }

    #[test]
    fn pins_and_preserves_explicit_file_and_quant_selection() {
        let plan = inspected_plan(TREE);
        let exact = select_from_plan(&plan, Some("demo-Q8_0.gguf"), None).unwrap();
        assert_eq!(exact.revision, "0123456789abcdef0123456789abcdef01234567");
        assert_eq!(exact.filename, "demo-Q8_0.gguf");

        let quant = select_from_plan(&plan, None, Some("q8_0")).unwrap();
        assert_eq!(quant.filename, "demo-Q8_0.gguf");
    }

    #[test]
    fn omitted_selection_rejects_zero_eligible_identities() {
        let tree = serde_json::json!([
            {
                "type": "file",
                "path": "split-00001-of-00002.gguf",
                "size": 2,
                "lfs": {"oid": "c".repeat(64), "size": 2}
            }
        ])
        .to_string();
        let error = select_from_plan(&inspected_plan(&tree), None, None).unwrap_err();

        assert_eq!(
            error.to_string(),
            "GGUF selection requires --file or --quant"
        );
    }

    #[test]
    fn omitted_selection_rejects_one_eligible_non_q4_file() {
        let tree = serde_json::json!([
            {
                "type": "file",
                "path": "demo-Q8_0.gguf",
                "size": 8,
                "lfs": {"oid": "b".repeat(64), "size": 8}
            }
        ])
        .to_string();
        let error = select_from_plan(&inspected_plan(&tree), None, None).unwrap_err();

        assert_eq!(
            error.to_string(),
            "GGUF selection requires --file or --quant"
        );
    }

    #[test]
    fn omitted_selection_rejects_unique_q4_among_multiple_eligible_files() {
        let error = select_from_plan(&inspected_plan(TREE), None, None).unwrap_err();

        assert_eq!(
            error.to_string(),
            "GGUF selection requires --file or --quant"
        );
    }

    #[test]
    fn resolved_file_semantic_accessors_are_exact() {
        let file = ResolvedFile {
            repo: "owner/repo".into(),
            revision: "0123456789abcdef0123456789abcdef01234567".into(),
            filename: "model-Q4_K_M.gguf".into(),
            sha256: "a".repeat(64),
            size: 42,
        };

        assert_eq!(file.repo(), "owner/repo");
        assert_eq!(file.commit(), "0123456789abcdef0123456789abcdef01234567");
        assert_eq!(file.path(), "model-Q4_K_M.gguf");
        assert_eq!(file.sha256(), "a".repeat(64));
        assert_eq!(file.size(), 42);
    }

    #[test]
    fn remote_repository_and_filename_reject_control_characters() {
        assert!(discovery::validate_repository("owner/repo\u{1b}").is_err());

        let controlled = TREE.replace("demo-Q4_K_M.gguf", "demo-\u{85}Q4_K_M.gguf");
        assert!(select_from_plan(
            &inspected_plan(&controlled),
            Some("demo-\u{85}Q4_K_M.gguf"),
            None,
        )
        .is_err());
    }

    #[test]
    fn missing_quant_lists_available_quantizations() {
        let error = select_from_plan(&inspected_plan(TREE), None, Some("NOT_A_QUANT")).unwrap_err();

        assert_eq!(
            error.to_string(),
            "quantization \"NOT_A_QUANT\" is not available; available quantizations: Q4_K_M, Q8_0. Retry with --quant <one of these values>."
        );
    }

    #[test]
    fn ambiguous_quant_names_matching_files_and_directs_to_file() {
        let ambiguous = inspected_plan(&TREE.replace("demo-Q8_0.gguf", "other-Q4_K_M.gguf"));
        let error = select_from_plan(&ambiguous, None, Some("Q4_K_M")).unwrap_err();

        assert_eq!(
            error.to_string(),
            "quantization \"Q4_K_M\" matched multiple files: demo-Q4_K_M.gguf, other-Q4_K_M.gguf. Use --file <filename> to choose one."
        );
    }

    #[test]
    fn mixed_case_gguf_extension_preserves_explicit_quant_selection() {
        let plan = inspected_plan(&TREE.replace("demo-Q4_K_M.gguf", "model-Q4_K_M.GgUf"));

        assert_eq!(
            select_from_plan(&plan, None, Some("Q4_K_M"))
                .unwrap()
                .path(),
            "model-Q4_K_M.GgUf"
        );
    }

    #[test]
    fn bearer_is_scoped_to_exact_huggingface_origin_and_is_sensitive() {
        let client = reqwest::blocking::Client::new();
        let hf = Url::parse("https://huggingface.co/api/models/owner/repo").unwrap();
        let other = Url::parse("https://cdn.example/model.gguf").unwrap();
        let hf_request = authorized_request(&client, hf, Some("secret"))
            .build()
            .unwrap();
        let other_request = authorized_request(&client, other, Some("secret"))
            .build()
            .unwrap();

        assert_eq!(hf_request.headers()[AUTHORIZATION], "Bearer secret");
        assert!(other_request.headers().get(AUTHORIZATION).is_none());
        assert!(!format!("{hf_request:?}").contains("secret"));
    }

    #[test]
    fn revision_path_segment_encoding_is_preserved() {
        assert!(metadata_url("owner/repo", Some("refs/pr/1"))
            .unwrap()
            .as_str()
            .contains("/revision/refs%2Fpr%2F1"));
        assert!(
            root_tree_url("owner/repo", "0123456789abcdef0123456789abcdef01234567")
                .unwrap()
                .as_str()
                .contains("owner/repo/tree/0123456789abcdef0123456789abcdef01234567")
        );
        let file = ResolvedFile {
            repo: "owner/repo".into(),
            revision: "0123456789abcdef0123456789abcdef01234567".into(),
            filename: "model name.gguf".into(),
            sha256: "a".repeat(64),
            size: 1,
        };
        assert!(crate::download::artifact_url(&file)
            .unwrap()
            .as_str()
            .ends_with("/model%20name.gguf"));
        assert!(validate_next_tree_url(
            "https://evil.example/page",
            "owner/repo",
            "0123456789abcdef0123456789abcdef01234567"
        )
        .is_err());
    }

    #[test]
    fn repository_plan_identity_matches_plan_repo_commit_path_oid_and_size() {
        let plan = inspected_plan(TREE);
        assert_eq!(plan.repo(), "owner/repo");
        assert_eq!(plan.commit(), "0123456789abcdef0123456789abcdef01234567");
        let expected = [
            ("demo-Q4_K_M.gguf", "a".repeat(64), 4),
            ("demo-Q8_0.gguf", "b".repeat(64), 8),
        ];
        let eligible = plan
            .candidates()
            .iter()
            .filter(|candidate| candidate.identity().is_some())
            .collect::<Vec<_>>();

        assert_eq!(eligible.len(), expected.len());
        for (candidate, (path, oid, size)) in eligible.into_iter().zip(expected.iter()) {
            let identity = candidate.identity().expect("eligible identity");
            assert_eq!(identity.repo(), plan.repo());
            assert_eq!(identity.commit(), plan.commit());
            assert_eq!(candidate.display_path(), *path);
            assert_eq!(candidate.size(), Some(*size));
            assert_eq!(identity.path(), *path);
            assert_eq!(identity.sha256(), oid);
            assert_eq!(identity.size(), *size);
        }
    }

    #[test]
    fn legacy_resolve_selects_from_the_complete_plan_without_refetch_or_revalidation() {
        let mut transport = FakeDiscoveryTransport::queued(vec![
            FakeDiscoveryTransport::json_response(StatusCode::OK, MODEL),
            FakeDiscoveryTransport::json_response(StatusCode::OK, TREE),
        ]);

        let selected = resolve_with_transport(
            InspectRepository::new("owner/repo".into(), None),
            Some("demo-Q4_K_M.gguf"),
            None,
            &mut transport,
        )
        .unwrap();

        assert_eq!(selected.path(), "demo-Q4_K_M.gguf");
        assert_eq!(transport.requests.len(), 2);
        assert_eq!(transport.requests[0].url.path(), "/api/models/owner/repo");
        assert_eq!(
            transport.requests[1].url.query_pairs().collect::<Vec<_>>(),
            vec![
                ("recursive".into(), "false".into()),
                ("expand".into(), "true".into()),
                ("limit".into(), "100".into()),
            ]
        );
    }

    #[test]
    fn compact_pull_and_flag_forms_resolve_to_the_same_resolved_file() {
        for (reference, filename, quant) in [
            (
                "hf.co/owner/repo:demo-Q4_K_M.gguf",
                Some("demo-Q4_K_M.gguf"),
                None,
            ),
            ("huggingface.co/owner/repo:q4_k_m", None, Some("q4_k_m")),
        ] {
            let compact = crate::cli::parse_pull_input(&crate::cli::PullArgs {
                repo: reference.into(),
                revision: None,
                filename: None,
                quant: None,
                name: None,
            })
            .unwrap();
            let responses = || {
                vec![
                    FakeDiscoveryTransport::json_response(StatusCode::OK, MODEL),
                    FakeDiscoveryTransport::json_response(StatusCode::OK, TREE),
                ]
            };
            let mut compact_transport = FakeDiscoveryTransport::queued(responses());
            let mut flag_transport = FakeDiscoveryTransport::queued(responses());

            let compact_resolved = resolve_with_transport(
                InspectRepository::new(compact.repo, compact.revision),
                compact.filename.as_deref(),
                compact.quant.as_deref(),
                &mut compact_transport,
            )
            .unwrap();
            let flag_resolved = resolve_with_transport(
                InspectRepository::new("owner/repo".into(), None),
                filename,
                quant,
                &mut flag_transport,
            )
            .unwrap();

            assert_eq!(compact_resolved, flag_resolved, "{reference}");
            assert_eq!(
                compact_transport.requests.len(),
                flag_transport.requests.len()
            );
            for (compact_request, flag_request) in compact_transport
                .requests
                .iter()
                .zip(&flag_transport.requests)
            {
                assert_eq!(compact_request.url, flag_request.url);
                assert_eq!(compact_request.timeout, flag_request.timeout);
                assert_eq!(compact_request.authorization, flag_request.authorization);
            }
        }
    }

    #[test]
    fn explicit_file_rejects_ineligible_packaging_and_case_mismatch() {
        let tree = serde_json::json!([
            {"type":"file","path":"Model-Q4_K_M.gguf","size":4,"lfs":{"oid":"a".repeat(64),"size":4}},
            {"type":"file","path":"model-draft.gguf","size":4,"lfs":{"oid":"b".repeat(64),"size":4}},
            {"type":"file","path":"nested/model.gguf","size":4,"lfs":{"oid":"c".repeat(64),"size":4}},
            {"type":"file","path":"model-00001-of-00002.gguf","size":4,"lfs":{"oid":"d".repeat(64),"size":4}},
            {"type":"file","path":"../unsafe.gguf","size":4,"lfs":{"oid":"e".repeat(64),"size":4}},
            {"type":"directory","path":"directory.gguf","size":4,"lfs":{"oid":"f".repeat(64),"size":4}},
            {"type":"file","path":"missing-size.gguf","lfs":{"oid":"1".repeat(64),"size":4}},
            {"type":"file","path":"missing-identity.gguf","size":4},
            {"type":"file","path":"size-mismatch.gguf","size":4,"lfs":{"oid":"2".repeat(64),"size":3}},
            {"type":"file","path":"zero-size.gguf","size":0,"lfs":{"oid":"3".repeat(64),"size":0}},
            {"type":"file","path":"invalid-hash.gguf","size":4,"lfs":{"oid":"not-a-sha","size":4}}
        ])
        .to_string();
        let plan = inspected_plan(&tree);

        for path in [
            "model-draft.gguf",
            "nested/model.gguf",
            "model-00001-of-00002.gguf",
            "../unsafe.gguf",
            "directory.gguf",
            "missing-size.gguf",
            "missing-identity.gguf",
            "size-mismatch.gguf",
            "zero-size.gguf",
            "invalid-hash.gguf",
            "model-Q4_K_M.gguf",
        ] {
            assert_eq!(
                select_from_plan(&plan, Some(path), None)
                    .unwrap_err()
                    .to_string(),
                format!("verified file {path:?} not found")
            );
        }
    }

    #[test]
    fn explicit_selectors_preserve_zero_eligible_plan_error() {
        let tree = serde_json::json!([
            {
                "type": "file",
                "path": "split-00001-of-00002.gguf",
                "size": 2,
                "lfs": {"oid": "c".repeat(64), "size": 2}
            }
        ])
        .to_string();
        let plan = inspected_plan(&tree);

        for (filename, quant) in [
            (Some("split-00001-of-00002.gguf"), None),
            (None, Some("Q4_K_M")),
        ] {
            assert_eq!(
                select_from_plan(&plan, filename, quant)
                    .unwrap_err()
                    .to_string(),
                "repository has no verified single-file GGUF"
            );
        }
    }

    #[test]
    fn token_discovery_child() {
        let Some(expected) = std::env::var_os("LOXA_TOKEN_DISCOVERY_EXPECT") else {
            return;
        };
        let expected = expected.to_string_lossy();
        let expected = (expected != "none").then_some(expected.as_ref());
        assert_eq!(discover_token().as_deref(), expected);
    }

    #[test]
    fn token_discovery_preserves_disable_token_path_and_home_precedence() {
        fn child(expected: &str, configure: impl FnOnce(&mut Command)) {
            let mut command = Command::new(std::env::current_exe().unwrap());
            command
                .arg("--exact")
                .arg("huggingface::tests::token_discovery_child")
                .arg("--nocapture")
                .env("LOXA_TOKEN_DISCOVERY_EXPECT", expected);
            for name in [
                "HF_HUB_DISABLE_IMPLICIT_TOKEN",
                "HF_TOKEN",
                "HF_TOKEN_PATH",
                "HF_HOME",
                "HOME",
            ] {
                command.env_remove(name);
            }
            configure(&mut command);
            assert!(command.status().unwrap().success());
        }

        let root = tempfile::tempdir().unwrap();
        let path_token = root.path().join("path-token");
        std::fs::write(&path_token, "path-token\n").unwrap();
        let hf_home = root.path().join("hf-home");
        std::fs::create_dir_all(&hf_home).unwrap();
        std::fs::write(hf_home.join("token"), "hf-home-token\n").unwrap();
        let home = root.path().join("home");
        std::fs::create_dir_all(home.join(".cache/huggingface")).unwrap();
        std::fs::write(home.join(".cache/huggingface/token"), "home-token\n").unwrap();

        child("none", |command| {
            command
                .env("HF_HUB_DISABLE_IMPLICIT_TOKEN", "1")
                .env("HF_TOKEN", "direct-token");
        });
        child("direct-token", |command| {
            command
                .env("HF_TOKEN", "direct-token")
                .env("HF_TOKEN_PATH", &path_token);
        });
        child("path-token", |command| {
            command
                .env("HF_TOKEN", "  ")
                .env("HF_TOKEN_PATH", &path_token)
                .env("HF_HOME", &hf_home)
                .env("HOME", &home);
        });
        child("hf-home-token", |command| {
            command.env("HF_HOME", &hf_home).env("HOME", &home);
        });
        child("home-token", |command| {
            command.env("HOME", &home);
        });
    }

    #[test]
    fn token_discovery_uses_an_immutable_environment_input() {
        let root = tempfile::tempdir().unwrap();
        let token_path = root.path().join("token");
        std::fs::write(&token_path, "path-token\n").unwrap();

        let direct = EnvironmentInput {
            token: Some(" direct-token ".into()),
            token_path: Some(token_path.clone().into_os_string()),
            ..EnvironmentInput::default()
        };
        assert_eq!(
            discover_token_from(&direct).as_deref(),
            Some("direct-token")
        );

        let disabled = EnvironmentInput {
            disable_implicit_token: Some("1".into()),
            token: Some("direct-token".into()),
            token_path: Some(token_path.into_os_string()),
            ..EnvironmentInput::default()
        };
        assert_eq!(discover_token_from(&disabled), None);
    }

    #[test]
    fn keyword_search_sends_the_exact_query_pairs_once() {
        let mut transport = FakeDiscoveryTransport::json(
            StatusCode::OK,
            r#"[{"id":"owner/repo","gated":false,"downloads":7}]"#,
        );

        let page = search_models_with_transport(
            SearchModels::new("two words & punctuation".into()),
            &mut transport,
        )
        .unwrap();

        assert_eq!(page.hits().len(), 1);
        assert_eq!(transport.requests.len(), 1);
        let request = &transport.requests[0];
        assert_eq!(request.url.path(), "/api/models");
        assert_eq!(
            request.url.query_pairs().collect::<Vec<_>>(),
            vec![
                ("search".into(), "two words & punctuation".into()),
                ("filter".into(), "gguf".into()),
                ("sort".into(), "downloads".into()),
                ("direction".into(), "-1".into()),
                ("limit".into(), "20".into()),
            ]
        );
    }

    #[test]
    fn keyword_search_maps_gating_and_sorts_deterministically() {
        let mut transport = FakeDiscoveryTransport::json(
            StatusCode::OK,
            r#"[
                {"id":"owner/z-public","gated":false,"downloads":10},
                {"id":"owner/a-auto","gated":"auto","downloads":11},
                {"id":"owner/b-manual","gated":"manual","downloads":10},
                {"id":"owner/c-other","gated":true,"downloads":2},
                {"id":"owner/d-missing"}
            ]"#,
        );

        let page = search_models_with_transport(
            SearchModels::new("ordinary query".into()),
            &mut transport,
        )
        .unwrap();
        let observed = page
            .hits()
            .iter()
            .map(|hit| (hit.repo(), hit.gated(), hit.downloads()))
            .collect::<Vec<_>>();

        assert_eq!(
            observed,
            vec![
                ("owner/a-auto", GatedStatus::AutomaticApproval, Some(11)),
                ("owner/b-manual", GatedStatus::ManualApproval, Some(10)),
                ("owner/z-public", GatedStatus::Public, Some(10)),
                ("owner/c-other", GatedStatus::Unknown, Some(2)),
                ("owner/d-missing", GatedStatus::Unknown, None),
            ]
        );
    }

    #[test]
    fn keyword_search_collapses_exact_duplicates_and_rejects_conflicts() {
        let mut identical = FakeDiscoveryTransport::json(
            StatusCode::OK,
            r#"[
                {"id":"owner/repo","gated":false,"downloads":7},
                {"id":"owner/repo","gated":false,"downloads":7}
            ]"#,
        );
        let page = search_models_with_transport(
            SearchModels::new("ordinary query".into()),
            &mut identical,
        )
        .unwrap();
        assert_eq!(page.hits().len(), 1);
        assert_eq!(page.hits()[0].repo(), "owner/repo");

        let mut conflicting = FakeDiscoveryTransport::json(
            StatusCode::OK,
            r#"[
                {"id":"owner/repo","gated":false,"downloads":7},
                {"id":"owner/repo","gated":"manual","downloads":7}
            ]"#,
        );
        let error = search_models_with_transport(
            SearchModels::new("ordinary query".into()),
            &mut conflicting,
        )
        .unwrap_err();
        assert_eq!(error.kind(), DiscoveryErrorKind::MalformedResponse);
    }

    #[test]
    fn keyword_search_rejects_invalid_upstream_repository_ids() {
        let mut transport = FakeDiscoveryTransport::json(
            StatusCode::OK,
            r#"[{"id":"owner/","gated":false,"downloads":7}]"#,
        );

        let error = search_models_with_transport(
            SearchModels::new("ordinary query".into()),
            &mut transport,
        )
        .unwrap_err();

        assert_eq!(error.kind(), DiscoveryErrorKind::MalformedResponse);
    }

    #[test]
    fn keyword_search_rejects_more_than_twenty_before_deduplication() {
        let entries = (0..21)
            .map(|_| {
                serde_json::json!({
                    "id":"owner/duplicate",
                    "gated": false,
                    "downloads": 7,
                })
            })
            .collect::<Vec<_>>();
        let body = serde_json::to_string(&entries).unwrap();
        let mut oversized = FakeDiscoveryTransport::json(StatusCode::OK, &body);
        let error = search_models_with_transport(
            SearchModels::new("ordinary query".into()),
            &mut oversized,
        )
        .unwrap_err();
        assert_eq!(error.kind(), DiscoveryErrorKind::ResponseTooLarge);

        let mut empty = FakeDiscoveryTransport::json(StatusCode::OK, "[]");
        let page =
            search_models_with_transport(SearchModels::new("ordinary query".into()), &mut empty)
                .unwrap();
        assert!(page.hits().is_empty());
    }

    #[test]
    fn bounded_search_rejects_declared_and_streamed_oversize_bodies() {
        let (mut declared, declared_reads) = FakeDiscoveryTransport::response(
            StatusCode::OK,
            Some((SEARCH_BODY_CAP + 1) as u64),
            vec![b"[]".to_vec()],
        );
        let declared_error =
            search_models_with_transport(SearchModels::new("ordinary query".into()), &mut declared)
                .unwrap_err();
        assert_eq!(declared_error.kind(), DiscoveryErrorKind::ResponseTooLarge);
        assert_eq!(declared_reads.get(), 0);

        let oversized = vec![b' '; SEARCH_BODY_CAP + 1];
        let split = SEARCH_BODY_CAP / 2;
        let (mut streamed, streamed_reads) = FakeDiscoveryTransport::response(
            StatusCode::OK,
            None,
            vec![oversized[..split].to_vec(), oversized[split..].to_vec()],
        );
        let streamed_error =
            search_models_with_transport(SearchModels::new("ordinary query".into()), &mut streamed)
                .unwrap_err();
        assert_eq!(streamed_error.kind(), DiscoveryErrorKind::ResponseTooLarge);
        assert!(streamed_reads.get() > 1);

        let (mut lying, lying_reads) = FakeDiscoveryTransport::response(
            StatusCode::OK,
            Some(2),
            vec![oversized[..split].to_vec(), oversized[split..].to_vec()],
        );
        let lying_error =
            search_models_with_transport(SearchModels::new("ordinary query".into()), &mut lying)
                .unwrap_err();
        assert_eq!(lying_error.kind(), DiscoveryErrorKind::ResponseTooLarge);
        assert!(lying_reads.get() > 1);
    }

    #[test]
    fn search_maps_every_status_without_reading_error_bodies() {
        for (status, expected) in [
            (StatusCode::FOUND, DiscoveryErrorKind::RedirectRejected),
            (
                StatusCode::UNAUTHORIZED,
                DiscoveryErrorKind::AuthenticationRequired,
            ),
            (StatusCode::FORBIDDEN, DiscoveryErrorKind::AccessDenied),
            (StatusCode::NOT_FOUND, DiscoveryErrorKind::RemoteUnavailable),
            (
                StatusCode::TOO_MANY_REQUESTS,
                DiscoveryErrorKind::RateLimited,
            ),
            (
                StatusCode::BAD_REQUEST,
                DiscoveryErrorKind::RemoteUnavailable,
            ),
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                DiscoveryErrorKind::RemoteUnavailable,
            ),
        ] {
            let (mut transport, reads) =
                FakeDiscoveryTransport::response(status, Some(4), vec![b"body".to_vec()]);
            let error = search_models_with_transport(
                SearchModels::new("ordinary query".into()),
                &mut transport,
            )
            .unwrap_err();
            assert_eq!(error.kind(), expected, "{status}");
            assert_eq!(reads.get(), 0, "{status}");
        }
    }

    #[test]
    fn search_maps_connect_and_timeout_failures_without_leaking_details() {
        let mut unavailable = FakeDiscoveryTransport::failure(TransportFailure::RemoteUnavailable);
        let unavailable_error = search_models_with_transport(
            SearchModels::new("ordinary query".into()),
            &mut unavailable,
        )
        .unwrap_err();
        assert_eq!(
            unavailable_error.kind(),
            DiscoveryErrorKind::RemoteUnavailable
        );

        let mut timeout = FakeDiscoveryTransport::failure(TransportFailure::DeadlineExceeded);
        let timeout_error =
            search_models_with_transport(SearchModels::new("ordinary query".into()), &mut timeout)
                .unwrap_err();
        assert_eq!(timeout_error.kind(), DiscoveryErrorKind::DeadlineExceeded);
        assert!(!format!("{timeout_error:?}").contains("ordinary query"));
        assert!(!timeout_error.to_string().contains("ordinary query"));
    }

    #[test]
    fn blocking_reqwest_body_timeout_maps_to_deadline_without_leaking_details() {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::sync::mpsc;
        use std::thread;

        struct OtherErrorReader;

        impl Read for OtherErrorReader {
            fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("non-timeout body error"))
            }
        }

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (partial_sent, partial_received) = mpsc::channel();
        let (release_server, release_received) = mpsc::channel();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0; 1];
            stream.read_exact(&mut request).unwrap();
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n[")
                .unwrap();
            stream.flush().unwrap();
            partial_sent.send(()).unwrap();
            let _ = release_received.recv();
        });

        let client = Client::builder()
            .timeout(Duration::from_millis(100))
            .build()
            .unwrap();
        let mut transport = ReqwestDiscoveryTransport::with_client(client, None);
        let url = Url::parse(&format!("http://{address}/timeout")).expect("loopback URL is valid");
        let mut response = transport
            .get(TransportRequest {
                url,
                timeout: Duration::from_millis(100),
                authorization: false,
            })
            .unwrap();
        partial_received.recv().unwrap();
        let timeout_error =
            decode_json::<serde_json::Value>(&mut response, SEARCH_BODY_CAP, || Ok(()))
                .unwrap_err();
        release_server.send(()).unwrap();
        server.join().unwrap();

        let mut non_timeout_response = TransportResponse {
            status: StatusCode::OK,
            content_length: None,
            links: Vec::new(),
            body: Box::new(OtherErrorReader),
        };
        let non_timeout_error =
            decode_json::<serde_json::Value>(&mut non_timeout_response, SEARCH_BODY_CAP, || Ok(()))
                .unwrap_err();

        assert_eq!(timeout_error.kind(), DiscoveryErrorKind::DeadlineExceeded);
        assert_eq!(
            non_timeout_error.kind(),
            DiscoveryErrorKind::RemoteUnavailable
        );
        assert!(!timeout_error.to_string().contains("127.0.0.1"));
        assert!(!format!("{timeout_error:?}").contains("127.0.0.1"));
    }

    #[test]
    fn authorization_is_scoped_and_redacted() {
        let mut exact = FakeDiscoveryTransport::json(StatusCode::OK, "[]");
        exact.token_available = true;
        search_models_with_transport(SearchModels::new("ordinary query".into()), &mut exact)
            .unwrap();
        assert!(exact.requests[0].authorization);

        let mut foreign = FakeDiscoveryTransport::json(StatusCode::OK, "[]");
        foreign.token_available = true;
        send(
            &mut foreign,
            Url::parse("https://example.com/api/models").unwrap(),
            REQUEST_TIMEOUT,
        )
        .unwrap();
        assert!(!foreign.requests[0].authorization);

        let error = DiscoveryError::new(DiscoveryErrorKind::RemoteUnavailable);
        assert!(!format!("{error:?}").contains("secret"));
        assert!(!error.to_string().contains("secret"));
    }

    #[test]
    fn inspection_resolves_and_canonicalizes_one_full_commit() {
        let uppercase_commit = "ABCDEF0123456789ABCDEF0123456789ABCDEF01";
        let mut transport = FakeDiscoveryTransport::queued(vec![
            FakeDiscoveryTransport::json_response(
                StatusCode::OK,
                &format!(r#"{{"sha":"{uppercase_commit}"}}"#),
            ),
            FakeDiscoveryTransport::json_response(StatusCode::OK, "[]"),
        ]);

        let plan = inspect_repository_with_transport(
            InspectRepository::new("owner/repo".into(), Some("main".into())),
            &mut transport,
        )
        .unwrap();

        assert_eq!(plan.repo(), "owner/repo");
        assert_eq!(plan.commit(), uppercase_commit.to_ascii_lowercase());
        assert!(plan.candidates().is_empty());
        assert_eq!(transport.requests.len(), 2);
    }

    #[test]
    fn inspection_rejects_missing_or_invalid_full_commit_before_tree() {
        let metadata_bodies = [
            "{}".to_owned(),
            r#"{"sha":"too-short"}"#.to_owned(),
            format!(r#"{{"sha":"{}"}}"#, "z".repeat(40)),
        ];

        for body in metadata_bodies {
            let mut transport =
                FakeDiscoveryTransport::queued(vec![FakeDiscoveryTransport::json_response(
                    StatusCode::OK,
                    &body,
                )]);
            let error = inspect_repository_with_transport(
                InspectRepository::new("owner/repo".into(), None),
                &mut transport,
            )
            .unwrap_err();

            assert_eq!(
                error.kind(),
                DiscoveryErrorKind::MalformedResponse,
                "{body}"
            );
            assert_eq!(transport.requests.len(), 1, "{body}");
        }
    }

    #[test]
    fn inspection_rejects_dot_only_revision_without_transport() {
        for revision in [".", ".."] {
            let mut transport = FakeDiscoveryTransport::queued(Vec::new());
            let error = inspect_repository_with_transport(
                InspectRepository::new("owner/repo".into(), Some(revision.into())),
                &mut transport,
            )
            .unwrap_err();

            assert_eq!(
                error.kind(),
                DiscoveryErrorKind::InvalidRevision,
                "{revision}"
            );
            assert!(transport.requests.is_empty(), "{revision}");
        }
    }

    #[test]
    fn inspection_uses_exact_root_tree_query() {
        let commit = "0123456789abcdef0123456789abcdef01234567";
        let mut transport = FakeDiscoveryTransport::queued(vec![
            FakeDiscoveryTransport::json_response(
                StatusCode::OK,
                &format!(r#"{{"sha":"{commit}"}}"#),
            ),
            FakeDiscoveryTransport::json_response(StatusCode::OK, "[]"),
        ]);

        inspect_repository_with_transport(
            InspectRepository::new("owner/repo".into(), None),
            &mut transport,
        )
        .unwrap();

        let request = &transport.requests[1];
        assert_eq!(
            request.url.path(),
            format!("/api/models/owner/repo/tree/{commit}")
        );
        assert_eq!(
            request.url.query_pairs().collect::<Vec<_>>(),
            vec![
                ("recursive".into(), "false".into()),
                ("expand".into(), "true".into()),
                ("limit".into(), "100".into()),
            ]
        );
    }

    #[test]
    fn metadata_and_tree_use_independent_streaming_body_caps() {
        let commit = "0123456789abcdef0123456789abcdef01234567";
        let oversized_metadata = format!(
            r#"{{"sha":"{commit}","padding":"{}"}}"#,
            "x".repeat(1024 * 1024 + 1)
        );
        let mut metadata =
            FakeDiscoveryTransport::queued(vec![FakeDiscoveryTransport::json_response(
                StatusCode::OK,
                &oversized_metadata,
            )]);
        let metadata_error = inspect_repository_with_transport(
            InspectRepository::new("owner/repo".into(), None),
            &mut metadata,
        )
        .unwrap_err();
        assert_eq!(metadata_error.kind(), DiscoveryErrorKind::ResponseTooLarge);

        let metadata_split = oversized_metadata.len() / 2;
        let metadata_chunks = vec![
            oversized_metadata.as_bytes()[..metadata_split].to_vec(),
            oversized_metadata.as_bytes()[metadata_split..].to_vec(),
        ];
        let (missing_length_response, missing_length_reads) =
            FakeDiscoveryTransport::response_with_counter(
                StatusCode::OK,
                None,
                metadata_chunks.clone(),
                Vec::new(),
            );
        let mut missing_length_metadata =
            FakeDiscoveryTransport::queued(vec![missing_length_response]);
        let missing_length_error = inspect_repository_with_transport(
            InspectRepository::new("owner/repo".into(), None),
            &mut missing_length_metadata,
        )
        .unwrap_err();
        assert_eq!(
            missing_length_error.kind(),
            DiscoveryErrorKind::ResponseTooLarge
        );
        assert!(missing_length_reads.get() > 1);

        let (lying_length_response, lying_length_reads) =
            FakeDiscoveryTransport::response_with_counter(
                StatusCode::OK,
                Some(2),
                metadata_chunks,
                Vec::new(),
            );
        let mut lying_length_metadata = FakeDiscoveryTransport::queued(vec![lying_length_response]);
        let lying_length_error = inspect_repository_with_transport(
            InspectRepository::new("owner/repo".into(), None),
            &mut lying_length_metadata,
        )
        .unwrap_err();
        assert_eq!(
            lying_length_error.kind(),
            DiscoveryErrorKind::ResponseTooLarge
        );
        assert!(lying_length_reads.get() > 1);

        let large_tree = format!("[{}]", " ".repeat(1024 * 1024 + 1));
        let mut tree = FakeDiscoveryTransport::queued(vec![
            FakeDiscoveryTransport::json_response(
                StatusCode::OK,
                &format!(r#"{{"sha":"{commit}"}}"#),
            ),
            FakeDiscoveryTransport::json_response(StatusCode::OK, &large_tree),
        ]);
        inspect_repository_with_transport(
            InspectRepository::new("owner/repo".into(), None),
            &mut tree,
        )
        .unwrap();

        let oversized_tree = format!("[{}]", " ".repeat(TREE_BODY_CAP + 1));
        let mut declared_tree = FakeDiscoveryTransport::queued(vec![
            FakeDiscoveryTransport::json_response(
                StatusCode::OK,
                &format!(r#"{{"sha":"{commit}"}}"#),
            ),
            FakeDiscoveryTransport::json_response(StatusCode::OK, &oversized_tree),
        ]);
        let declared_tree_error = inspect_repository_with_transport(
            InspectRepository::new("owner/repo".into(), None),
            &mut declared_tree,
        )
        .unwrap_err();
        assert_eq!(
            declared_tree_error.kind(),
            DiscoveryErrorKind::ResponseTooLarge
        );

        let mut streamed_tree = FakeDiscoveryTransport::queued(vec![
            FakeDiscoveryTransport::json_response(
                StatusCode::OK,
                &format!(r#"{{"sha":"{commit}"}}"#),
            ),
            FakeDiscoveryTransport::json_response_without_length(StatusCode::OK, &oversized_tree),
        ]);
        let streamed_tree_error = inspect_repository_with_transport(
            InspectRepository::new("owner/repo".into(), None),
            &mut streamed_tree,
        )
        .unwrap_err();
        assert_eq!(
            streamed_tree_error.kind(),
            DiscoveryErrorKind::ResponseTooLarge
        );
    }

    #[test]
    fn inspection_maps_context_sensitive_not_found_and_all_statuses() {
        let (mut default_metadata, default_metadata_reads) = FakeDiscoveryTransport::response(
            StatusCode::NOT_FOUND,
            Some(4),
            vec![b"body".to_vec()],
        );
        let default_error = inspect_repository_with_transport(
            InspectRepository::new("owner/repo".into(), None),
            &mut default_metadata,
        )
        .unwrap_err();
        assert_eq!(default_error.kind(), DiscoveryErrorKind::RepositoryNotFound);
        assert_eq!(default_metadata_reads.get(), 0);

        let (mut revision_metadata, revision_metadata_reads) = FakeDiscoveryTransport::response(
            StatusCode::NOT_FOUND,
            Some(4),
            vec![b"body".to_vec()],
        );
        let revision_error = inspect_repository_with_transport(
            InspectRepository::new("owner/repo".into(), Some("branch".into())),
            &mut revision_metadata,
        )
        .unwrap_err();
        assert_eq!(revision_error.kind(), DiscoveryErrorKind::RevisionNotFound);
        assert_eq!(revision_metadata_reads.get(), 0);

        let commit = "0123456789abcdef0123456789abcdef01234567";
        let (tree_response, tree_reads) = FakeDiscoveryTransport::response_with_counter(
            StatusCode::NOT_FOUND,
            Some(4),
            vec![b"body".to_vec()],
            Vec::new(),
        );
        let mut tree = FakeDiscoveryTransport::queued(vec![
            FakeDiscoveryTransport::json_response(
                StatusCode::OK,
                &format!(r#"{{"sha":"{commit}"}}"#),
            ),
            tree_response,
        ]);
        let tree_error = inspect_repository_with_transport(
            InspectRepository::new("owner/repo".into(), None),
            &mut tree,
        )
        .unwrap_err();
        assert_eq!(tree_error.kind(), DiscoveryErrorKind::RepositoryNotFound);
        assert_eq!(tree_reads.get(), 0);

        for (status, expected) in [
            (StatusCode::FOUND, DiscoveryErrorKind::RedirectRejected),
            (
                StatusCode::UNAUTHORIZED,
                DiscoveryErrorKind::AuthenticationRequired,
            ),
            (StatusCode::FORBIDDEN, DiscoveryErrorKind::AccessDenied),
            (
                StatusCode::TOO_MANY_REQUESTS,
                DiscoveryErrorKind::RateLimited,
            ),
            (
                StatusCode::BAD_REQUEST,
                DiscoveryErrorKind::RemoteUnavailable,
            ),
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                DiscoveryErrorKind::RemoteUnavailable,
            ),
        ] {
            let (mut transport, reads) =
                FakeDiscoveryTransport::response(status, Some(4), vec![b"body".to_vec()]);
            let error = inspect_repository_with_transport(
                InspectRepository::new("owner/repo".into(), None),
                &mut transport,
            )
            .unwrap_err();
            assert_eq!(error.kind(), expected, "{status}");
            assert_eq!(reads.get(), 0, "{status}");
        }
    }

    #[test]
    fn pagination_accepts_one_exact_next_across_all_link_fields() {
        let commit = "0123456789abcdef0123456789abcdef01234567";
        let next = format!(
            "https://huggingface.co/api/models/owner/repo/tree/{commit}?recursive=false&expand=true&limit=100&cursor=next-cursor"
        );
        let mut transport = FakeDiscoveryTransport::queued(vec![
            FakeDiscoveryTransport::json_response(
                StatusCode::OK,
                &format!(r#"{{"sha":"{commit}"}}"#),
            ),
            FakeDiscoveryTransport::json_response_with_links(
                StatusCode::OK,
                "[]",
                vec![
                    HeaderValue::from_static("</ignored>; rel=\"prev alternate\""),
                    HeaderValue::from_str(&format!("<{next}>; rel=\"next alternate\"")).unwrap(),
                ],
            ),
            FakeDiscoveryTransport::json_response(StatusCode::OK, "[]"),
        ]);

        inspect_repository_with_transport(
            InspectRepository::new("owner/repo".into(), None),
            &mut transport,
        )
        .unwrap();

        assert_eq!(transport.requests.len(), 3);
        assert_eq!(transport.requests[2].url.as_str(), next);
    }

    #[test]
    fn pagination_rejects_malformed_or_multiple_next_before_request() {
        let commit = "0123456789abcdef0123456789abcdef01234567";
        let metadata = || {
            FakeDiscoveryTransport::json_response(
                StatusCode::OK,
                &format!(r#"{{"sha":"{commit}"}}"#),
            )
        };
        let target = format!(
            "https://huggingface.co/api/models/owner/repo/tree/{commit}?recursive=false&expand=true&limit=100&cursor=one"
        );
        let malformed_headers = vec![
            HeaderValue::from_bytes(b"\xff").expect("opaque header bytes"),
            HeaderValue::from_str(&format!("{target}; rel=\"prev\"")).unwrap(),
            HeaderValue::from_str(&format!("<{target}; rel=\"prev\"")).unwrap(),
            HeaderValue::from_str(&format!("<{target}> rel=\"prev\"")).unwrap(),
            HeaderValue::from_str(&format!("<{target}>; rel")).unwrap(),
            HeaderValue::from_str(&format!("<{target}>; rel=\"unterminated")).unwrap(),
            HeaderValue::from_str(&format!("<{target}>; rel=\"next\\")).unwrap(),
        ];
        for header in malformed_headers {
            let mut transport = FakeDiscoveryTransport::queued(vec![
                metadata(),
                FakeDiscoveryTransport::json_response_with_links(
                    StatusCode::OK,
                    "[]",
                    vec![header],
                ),
            ]);
            let error = inspect_repository_with_transport(
                InspectRepository::new("owner/repo".into(), None),
                &mut transport,
            )
            .unwrap_err();
            assert_eq!(error.kind(), DiscoveryErrorKind::PaginationRejected);
            assert_eq!(transport.requests.len(), 2);
        }

        let second_target = target.replacen("cursor=one", "cursor=two", 1);
        let multiple = HeaderValue::from_str(&format!(
            "<{target}>; rel=\"next\", <{second_target}>; rel=\"next\""
        ))
        .unwrap();
        let mut multiple_transport = FakeDiscoveryTransport::queued(vec![
            metadata(),
            FakeDiscoveryTransport::json_response_with_links(StatusCode::OK, "[]", vec![multiple]),
        ]);
        let multiple_error = inspect_repository_with_transport(
            InspectRepository::new("owner/repo".into(), None),
            &mut multiple_transport,
        )
        .unwrap_err();
        assert_eq!(
            multiple_error.kind(),
            DiscoveryErrorKind::PaginationRejected
        );
        assert_eq!(multiple_transport.requests.len(), 2);
    }

    #[test]
    fn pagination_ignores_well_formed_substring_only_relations() {
        let commit = "0123456789abcdef0123456789abcdef01234567";
        let target = format!(
            "https://huggingface.co/api/models/owner/repo/tree/{commit}?recursive=false&expand=true&limit=100&cursor=not-next"
        );
        let relations = HeaderValue::from_str(&format!(
            "<{target}>; rel=\"pre-next\", <{target}>; rel=\"nextish\""
        ))
        .unwrap();
        let mut transport = FakeDiscoveryTransport::queued(vec![
            FakeDiscoveryTransport::json_response(
                StatusCode::OK,
                &format!(r#"{{"sha":"{commit}"}}"#),
            ),
            FakeDiscoveryTransport::json_response_with_links(StatusCode::OK, "[]", vec![relations]),
        ]);

        let plan = inspect_repository_with_transport(
            InspectRepository::new("owner/repo".into(), None),
            &mut transport,
        )
        .unwrap();
        assert!(plan.candidates().is_empty());
        assert_eq!(transport.requests.len(), 2);
    }

    #[test]
    fn pagination_rejects_malformed_non_next_targets_and_padded_relations_before_request() {
        let commit = "0123456789abcdef0123456789abcdef01234567";
        let target = format!(
            "https://huggingface.co/api/models/owner/repo/tree/{commit}?recursive=false&expand=true&limit=100&cursor=next"
        );
        let mut headers = vec![(
            "whitespace non-next".to_owned(),
            "<not a URI>; rel=\"prev\"".to_owned(),
        )];
        for (description, cursor) in [
            ("dangling percent", "dangling%"),
            ("incomplete percent", "incomplete%1"),
            ("raw backslash", r"back\slash"),
            ("nested angle bracket", "nested<value"),
            ("raw quote", r#"quote"value"#),
        ] {
            let malformed_target = target.replacen("cursor=next", &format!("cursor={cursor}"), 1);
            headers.push((
                format!("{description} non-next"),
                format!("<{malformed_target}>; rel=\"prev\""),
            ));
            headers.push((
                format!("{description} exact next"),
                format!("<{malformed_target}>; rel=\"next\""),
            ));
        }
        headers.extend([
            (
                "padded leading relation".to_owned(),
                format!("<{target}>; rel=\" next \""),
            ),
            (
                "padded trailing relation".to_owned(),
                format!("<{target}>; rel=\"next \""),
            ),
        ]);

        for (description, header) in headers {
            let mut transport = FakeDiscoveryTransport::queued(vec![
                FakeDiscoveryTransport::json_response(
                    StatusCode::OK,
                    &format!(r#"{{"sha":"{commit}"}}"#),
                ),
                FakeDiscoveryTransport::json_response_with_links(
                    StatusCode::OK,
                    "[]",
                    vec![HeaderValue::from_str(&header).expect("valid header field bytes")],
                ),
            ]);
            transport.token_available = true;
            let result = inspect_repository_with_transport(
                InspectRepository::new("owner/repo".into(), None),
                &mut transport,
            );
            assert_eq!(
                result.err().map(|error| error.kind()),
                Some(DiscoveryErrorKind::PaginationRejected),
                "{description}"
            );
            assert_eq!(transport.requests.len(), 2, "{description}");
            assert!(
                transport
                    .requests
                    .iter()
                    .all(|request| request.authorization),
                "{description}"
            );
        }
    }

    #[test]
    fn pagination_rejects_empty_or_repeated_relation_parameters_before_request() {
        let commit = "0123456789abcdef0123456789abcdef01234567";
        let target = format!(
            "https://huggingface.co/api/models/owner/repo/tree/{commit}?recursive=false&expand=true&limit=100&cursor=next"
        );
        let headers = [
            format!("<{target}>; rel=\"\""),
            format!("<{target}>; rel=\"   \""),
            format!("<{target}>; rel=\"next\"; rel=\"prev\""),
        ];
        let observed = headers
            .iter()
            .map(|header| {
                let mut transport = FakeDiscoveryTransport::queued(vec![
                    FakeDiscoveryTransport::json_response(
                        StatusCode::OK,
                        &format!(r#"{{"sha":"{commit}"}}"#),
                    ),
                    FakeDiscoveryTransport::json_response_with_links(
                        StatusCode::OK,
                        "[]",
                        vec![HeaderValue::from_str(header).unwrap()],
                    ),
                ]);
                let result = inspect_repository_with_transport(
                    InspectRepository::new("owner/repo".into(), None),
                    &mut transport,
                );
                (
                    result.err().map(|error| error.kind()),
                    transport.requests.len(),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(
            observed,
            vec![
                (Some(DiscoveryErrorKind::PaginationRejected), 2),
                (Some(DiscoveryErrorKind::PaginationRejected), 2),
                (Some(DiscoveryErrorKind::PaginationRejected), 2),
            ]
        );
    }

    #[test]
    fn pagination_rejects_foreign_or_credentialed_next_without_authorization() {
        let commit = "0123456789abcdef0123456789abcdef01234567";
        let exact = format!(
            "https://huggingface.co/api/models/owner/repo/tree/{commit}?recursive=false&expand=true&limit=100&cursor=next"
        );
        let targets = [
            exact.replacen("huggingface.co", "evil.example", 1),
            exact.replacen("https://huggingface.co", "http://huggingface.co", 1),
            exact.replacen("https://huggingface.co", "https://huggingface.co:444", 1),
            exact.replacen("https://", "https://credentials@", 1),
            format!("{exact}#fragment"),
            exact.replacen("/owner/repo/", "/other/repo/", 1),
            exact.replacen(commit, "abcdef0123456789abcdef0123456789abcdef01", 1),
        ];

        for target in targets {
            let mut transport = FakeDiscoveryTransport::queued(vec![
                FakeDiscoveryTransport::json_response(
                    StatusCode::OK,
                    &format!(r#"{{"sha":"{commit}"}}"#),
                ),
                FakeDiscoveryTransport::json_response_with_links(
                    StatusCode::OK,
                    "[]",
                    vec![HeaderValue::from_str(&format!("<{target}>; rel=\"next\"")).unwrap()],
                ),
            ]);
            transport.token_available = true;

            let error = inspect_repository_with_transport(
                InspectRepository::new("owner/repo".into(), None),
                &mut transport,
            )
            .unwrap_err();

            assert_eq!(
                error.kind(),
                DiscoveryErrorKind::PaginationRejected,
                "{target}"
            );
            assert_eq!(transport.requests.len(), 2, "{target}");
            assert!(transport
                .requests
                .iter()
                .all(|request| request.authorization
                    && request.url.host_str() == Some("huggingface.co")));
        }
    }

    #[test]
    fn pagination_rejects_raw_path_aliases_before_authorization() {
        let commit = "0123456789abcdef0123456789abcdef01234567";
        let exact = format!(
            "https://huggingface.co/api/models/owner/repo/tree/{commit}?recursive=false&expand=true&limit=100&cursor=next"
        );
        let targets = [
            exact.replacen("/owner/repo/", "/owner/extra/../repo/", 1),
            exact.replacen("/owner/repo/", "/owner/ignored/%2e%2e/repo/", 1),
            exact.replacen("/owner/repo/", "/owner\\repo/", 1),
            exact.replacen("/tree/", "/tree/ignored/../", 1),
        ];

        for target in targets {
            let mut transport = FakeDiscoveryTransport::queued(vec![
                FakeDiscoveryTransport::json_response(
                    StatusCode::OK,
                    &format!(r#"{{"sha":"{commit}"}}"#),
                ),
                FakeDiscoveryTransport::json_response_with_links(
                    StatusCode::OK,
                    "[]",
                    vec![HeaderValue::from_str(&format!("<{target}>; rel=\"next\"")).unwrap()],
                ),
            ]);
            transport.token_available = true;

            let error = inspect_repository_with_transport(
                InspectRepository::new("owner/repo".into(), None),
                &mut transport,
            )
            .unwrap_err();

            assert_eq!(
                error.kind(),
                DiscoveryErrorKind::PaginationRejected,
                "{target}"
            );
            assert_eq!(transport.requests.len(), 2, "{target}");
            assert!(transport
                .requests
                .iter()
                .all(|request| request.authorization
                    && request.url.host_str() == Some("huggingface.co")));
        }
    }

    #[test]
    fn pagination_rejects_duplicate_extra_or_missing_query_keys_before_request() {
        let commit = "0123456789abcdef0123456789abcdef01234567";
        let exact = format!(
            "https://huggingface.co/api/models/owner/repo/tree/{commit}?recursive=false&expand=true&limit=100&cursor=next"
        );
        let targets = [
            exact.replacen("recursive=false", "recursive=true", 1),
            exact.replacen("limit=100", "limit=500", 1),
            format!("{exact}&limit=100"),
            format!("{exact}&cursor=again"),
            format!("{exact}&unexpected=value"),
            exact.replacen("cursor=next", "cursor=", 1),
            exact.replacen("&cursor=next", "", 1),
        ];

        for target in targets {
            let mut transport = FakeDiscoveryTransport::queued(vec![
                FakeDiscoveryTransport::json_response(
                    StatusCode::OK,
                    &format!(r#"{{"sha":"{commit}"}}"#),
                ),
                FakeDiscoveryTransport::json_response_with_links(
                    StatusCode::OK,
                    "[]",
                    vec![HeaderValue::from_str(&format!("<{target}>; rel=\"next\"")).unwrap()],
                ),
            ]);
            transport.token_available = true;

            let error = inspect_repository_with_transport(
                InspectRepository::new("owner/repo".into(), None),
                &mut transport,
            )
            .unwrap_err();

            assert_eq!(
                error.kind(),
                DiscoveryErrorKind::PaginationRejected,
                "{target}"
            );
            assert_eq!(transport.requests.len(), 2, "{target}");
            assert!(transport
                .requests
                .iter()
                .all(|request| request.authorization
                    && request.url.host_str() == Some("huggingface.co")));
        }
    }

    #[test]
    fn inspection_enforces_page_entry_and_candidate_caps_without_truncation() {
        let commit = "0123456789abcdef0123456789abcdef01234567";
        let metadata = || {
            FakeDiscoveryTransport::json_response(
                StatusCode::OK,
                &format!(r#"{{"sha":"{commit}"}}"#),
            )
        };

        let page_101 = serde_json::to_string(&vec![serde_json::json!({}); 101]).unwrap();
        let mut too_many_entries = FakeDiscoveryTransport::queued(vec![
            metadata(),
            FakeDiscoveryTransport::json_response(StatusCode::OK, &page_101),
        ]);
        let entry_error = inspect_repository_with_transport(
            InspectRepository::new("owner/repo".into(), None),
            &mut too_many_entries,
        )
        .unwrap_err();
        assert_eq!(entry_error.kind(), DiscoveryErrorKind::ResponseTooLarge);

        let link = |cursor: &str| {
            HeaderValue::from_str(&format!(
                "<https://huggingface.co/api/models/owner/repo/tree/{commit}?recursive=false&expand=true&limit=100&cursor={cursor}>; rel=\"next\""
            ))
            .unwrap()
        };
        let non_gguf_page = |start: usize| {
            serde_json::to_string(
                &(start..(start + 100))
                    .map(|index| serde_json::json!({"type":"file","path":format!("notes-{index}.txt")}))
                    .collect::<Vec<_>>(),
            )
            .unwrap()
        };
        let mut complete_horizon = FakeDiscoveryTransport::queued(vec![
            metadata(),
            FakeDiscoveryTransport::json_response_with_links(
                StatusCode::OK,
                &non_gguf_page(0),
                vec![link("one")],
            ),
            FakeDiscoveryTransport::json_response_with_links(
                StatusCode::OK,
                &non_gguf_page(100),
                vec![link("two")],
            ),
            FakeDiscoveryTransport::json_response_with_links(
                StatusCode::OK,
                &non_gguf_page(200),
                vec![link("three")],
            ),
            FakeDiscoveryTransport::json_response(StatusCode::OK, &non_gguf_page(300)),
        ]);
        let complete_plan = inspect_repository_with_transport(
            InspectRepository::new("owner/repo".into(), None),
            &mut complete_horizon,
        )
        .unwrap();
        assert!(complete_plan.candidates().is_empty());
        assert_eq!(complete_horizon.requests.len(), 5);

        let candidate_page = |start: usize, count: usize| {
            serde_json::to_string(
                &(start..(start + count))
                    .map(|index| {
                        serde_json::json!({"type":"directory","path":format!("model-{index}.gguf")})
                    })
                    .collect::<Vec<_>>(),
            )
            .unwrap()
        };
        let mut too_many_candidates = FakeDiscoveryTransport::queued(vec![
            metadata(),
            FakeDiscoveryTransport::json_response_with_links(
                StatusCode::OK,
                &candidate_page(0, 100),
                vec![link("candidates-one")],
            ),
            FakeDiscoveryTransport::json_response_with_links(
                StatusCode::OK,
                &candidate_page(100, 100),
                vec![link("candidates-two")],
            ),
            FakeDiscoveryTransport::json_response(StatusCode::OK, &candidate_page(200, 57)),
        ]);
        let candidate_error = inspect_repository_with_transport(
            InspectRepository::new("owner/repo".into(), None),
            &mut too_many_candidates,
        )
        .unwrap_err();
        assert_eq!(candidate_error.kind(), DiscoveryErrorKind::ResponseTooLarge);
        assert_eq!(too_many_candidates.requests.len(), 4);

        let mut fifth_page = FakeDiscoveryTransport::queued(vec![
            metadata(),
            FakeDiscoveryTransport::json_response_with_links(
                StatusCode::OK,
                &non_gguf_page(0),
                vec![link("one")],
            ),
            FakeDiscoveryTransport::json_response_with_links(
                StatusCode::OK,
                &non_gguf_page(100),
                vec![link("two")],
            ),
            FakeDiscoveryTransport::json_response_with_links(
                StatusCode::OK,
                &non_gguf_page(200),
                vec![link("three")],
            ),
            FakeDiscoveryTransport::json_response_with_links(
                StatusCode::OK,
                &non_gguf_page(300),
                vec![link("four")],
            ),
        ]);
        let page_error = inspect_repository_with_transport(
            InspectRepository::new("owner/repo".into(), None),
            &mut fifth_page,
        )
        .unwrap_err();
        assert_eq!(page_error.kind(), DiscoveryErrorKind::ResponseTooLarge);
        assert_eq!(fifth_page.requests.len(), 5);
    }

    #[test]
    fn pagination_rejects_canonical_cycles_before_request() {
        let commit = "0123456789abcdef0123456789abcdef01234567";
        let repeated = HeaderValue::from_static(
            "<https://huggingface.co/api/models/owner/repo/tree/0123456789abcdef0123456789abcdef01234567?recursive=false&expand=true&limit=100&cursor=repeat>; rel=\"next\"",
        );
        let mut transport = FakeDiscoveryTransport::queued(vec![
            FakeDiscoveryTransport::json_response(
                StatusCode::OK,
                &format!(r#"{{"sha":"{commit}"}}"#),
            ),
            FakeDiscoveryTransport::json_response_with_links(
                StatusCode::OK,
                "[]",
                vec![repeated.clone()],
            ),
            FakeDiscoveryTransport::json_response_with_links(StatusCode::OK, "[]", vec![repeated]),
        ]);

        let error = inspect_repository_with_transport(
            InspectRepository::new("owner/repo".into(), None),
            &mut transport,
        )
        .unwrap_err();
        assert_eq!(error.kind(), DiscoveryErrorKind::PaginationRejected);
        assert_eq!(transport.requests.len(), 3);

        let percent_encoded_cursor = HeaderValue::from_str(&format!(
            "<https://huggingface.co/api/models/owner/repo/tree/{commit}?expand=true&cursor=%72epeat&limit=100&recursive=false>; rel=\"next\""
        ))
        .unwrap();
        let reordered_cursor = HeaderValue::from_str(&format!(
            "<https://huggingface.co/api/models/owner/repo/tree/{commit}?limit=100&recursive=false&cursor=repeat&expand=true>; rel=\"next\""
        ))
        .unwrap();
        let mut equivalent_transport = FakeDiscoveryTransport::queued(vec![
            FakeDiscoveryTransport::json_response(
                StatusCode::OK,
                &format!(r#"{{"sha":"{commit}"}}"#),
            ),
            FakeDiscoveryTransport::json_response_with_links(
                StatusCode::OK,
                "[]",
                vec![percent_encoded_cursor],
            ),
            FakeDiscoveryTransport::json_response_with_links(
                StatusCode::OK,
                "[]",
                vec![reordered_cursor],
            ),
        ]);

        let equivalent_error = inspect_repository_with_transport(
            InspectRepository::new("owner/repo".into(), None),
            &mut equivalent_transport,
        )
        .unwrap_err();
        assert_eq!(
            equivalent_error.kind(),
            DiscoveryErrorKind::PaginationRejected
        );
        assert_eq!(equivalent_transport.requests.len(), 3);
    }

    #[test]
    fn inspection_enforces_request_and_overall_deadlines_with_fake_time() {
        let commit = "0123456789abcdef0123456789abcdef01234567";
        let mut zero_transport = FakeDiscoveryTransport::queued(Vec::new());
        let mut zero_clock = FakeClock::new([Duration::from_secs(45)]);
        let zero_error = inspect_repository_with_transport_and_clock(
            InspectRepository::new("owner/repo".into(), None),
            &mut zero_transport,
            &mut zero_clock,
        )
        .unwrap_err();
        assert_eq!(zero_error.kind(), DiscoveryErrorKind::DeadlineExceeded);
        assert!(zero_transport.requests.is_empty());

        let mut transport = FakeDiscoveryTransport::queued(vec![
            FakeDiscoveryTransport::json_response(
                StatusCode::OK,
                &format!(r#"{{"sha":"{commit}"}}"#),
            ),
            FakeDiscoveryTransport::json_response(StatusCode::OK, "[]"),
        ]);
        let mut clock = FakeClock::new([
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            Duration::from_secs(35),
            Duration::from_secs(35),
            Duration::from_secs(45),
        ]);
        let error = inspect_repository_with_transport_and_clock(
            InspectRepository::new("owner/repo".into(), None),
            &mut transport,
            &mut clock,
        )
        .unwrap_err();
        assert_eq!(error.kind(), DiscoveryErrorKind::DeadlineExceeded);
        assert_eq!(
            transport
                .requests
                .iter()
                .map(|request| request.timeout)
                .collect::<Vec<_>>(),
            vec![Duration::from_secs(15), Duration::from_secs(10)]
        );
    }

    #[test]
    fn inspection_gives_deadline_precedence_after_completed_work() {
        let commit = "0123456789abcdef0123456789abcdef01234567";
        let metadata = || {
            FakeDiscoveryTransport::json_response(
                StatusCode::OK,
                &format!(r#"{{"sha":"{commit}"}}"#),
            )
        };

        let streamed_oversize = format!("[{}]", " ".repeat(TREE_BODY_CAP + 1));
        let mut streamed_transport = FakeDiscoveryTransport::queued(vec![
            metadata(),
            FakeDiscoveryTransport::json_response_without_length(
                StatusCode::OK,
                &streamed_oversize,
            ),
        ]);
        let mut streamed_clock = FakeClock::new([
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            Duration::from_secs(45),
        ]);
        let streamed = inspect_repository_with_transport_and_clock(
            InspectRepository::new("owner/repo".into(), None),
            &mut streamed_transport,
            &mut streamed_clock,
        );

        let mut malformed_transport = FakeDiscoveryTransport::queued(vec![
            metadata(),
            FakeDiscoveryTransport::json_response(StatusCode::OK, "{"),
        ]);
        let mut malformed_clock = FakeClock::new([
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            Duration::from_secs(45),
        ]);
        let malformed = inspect_repository_with_transport_and_clock(
            InspectRepository::new("owner/repo".into(), None),
            &mut malformed_transport,
            &mut malformed_clock,
        );

        let previous = HeaderValue::from_static("<https://huggingface.co/ignored>; rel=\"prev\"");
        let mut link_transport = FakeDiscoveryTransport::queued(vec![
            metadata(),
            FakeDiscoveryTransport::json_response_with_links(StatusCode::OK, "[]", vec![previous]),
        ]);
        let mut link_clock = FakeClock::new([
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            Duration::from_secs(45),
        ]);
        let link = inspect_repository_with_transport_and_clock(
            InspectRepository::new("owner/repo".into(), None),
            &mut link_transport,
            &mut link_clock,
        );

        let final_tree = serde_json::json!([
            {"type":"file","path":"model.gguf","size":4,"lfs":{"oid":"a".repeat(64),"size":4}}
        ])
        .to_string();
        let mut candidate_transport = FakeDiscoveryTransport::queued(vec![
            metadata(),
            FakeDiscoveryTransport::json_response(StatusCode::OK, &final_tree),
        ]);
        let mut candidate_clock = FakeClock::new([
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            Duration::from_secs(45),
        ]);
        let candidate = inspect_repository_with_transport_and_clock(
            InspectRepository::new("owner/repo".into(), None),
            &mut candidate_transport,
            &mut candidate_clock,
        );

        assert_eq!(
            vec![
                (
                    streamed.err().map(|error| error.kind()),
                    streamed_transport.requests.len()
                ),
                (
                    malformed.err().map(|error| error.kind()),
                    malformed_transport.requests.len()
                ),
                (
                    link.err().map(|error| error.kind()),
                    link_transport.requests.len()
                ),
                (
                    candidate.err().map(|error| error.kind()),
                    candidate_transport.requests.len()
                ),
            ],
            vec![
                (Some(DiscoveryErrorKind::DeadlineExceeded), 2),
                (Some(DiscoveryErrorKind::DeadlineExceeded), 2),
                (Some(DiscoveryErrorKind::DeadlineExceeded), 2),
                (Some(DiscoveryErrorKind::DeadlineExceeded), 2),
            ]
        );
    }

    #[test]
    fn remote_auxiliary_classification_uses_the_shared_helper() {
        let commit = "0123456789abcdef0123456789abcdef01234567";
        let oid = "a".repeat(64);
        let tree = serde_json::json!([
            {"type":"file","path":"mtp-model.gguf","size":4,"lfs":{"oid":oid,"size":4}},
            {"type":"file","path":"model-draft.gguf","size":4,"lfs":{"oid":"b".repeat(64),"size":4}},
            {"type":"file","path":"model-mmproj.gguf","size":4,"lfs":{"oid":"c".repeat(64),"size":4}},
            {"type":"file","path":"model-MtP-draft.mmproj.gguf","size":4,"lfs":{"oid":"d".repeat(64),"size":4}}
        ]);
        let mut transport = FakeDiscoveryTransport::queued(vec![
            FakeDiscoveryTransport::json_response(
                StatusCode::OK,
                &format!(r#"{{"sha":"{commit}"}}"#),
            ),
            FakeDiscoveryTransport::json_response(StatusCode::OK, &tree.to_string()),
        ]);

        let plan = inspect_repository_with_transport(
            InspectRepository::new("owner/repo".into(), None),
            &mut transport,
        )
        .unwrap();
        let dispositions = plan
            .candidates()
            .iter()
            .map(|candidate| candidate.disposition())
            .collect::<Vec<_>>();
        assert_eq!(
            dispositions,
            vec![
                CandidateDisposition::UnsupportedPackaging(UnsupportedPackagingReason::Auxiliary(
                    AuxiliaryRole::Mtp,
                )),
                CandidateDisposition::UnsupportedPackaging(UnsupportedPackagingReason::Auxiliary(
                    AuxiliaryRole::Draft,
                )),
                CandidateDisposition::UnsupportedPackaging(UnsupportedPackagingReason::Auxiliary(
                    AuxiliaryRole::Mmproj,
                )),
                CandidateDisposition::UnsupportedPackaging(UnsupportedPackagingReason::Auxiliary(
                    AuxiliaryRole::Mtp,
                )),
            ]
        );
    }

    #[test]
    fn candidate_disposition_uses_the_exact_first_failure_precedence() {
        let commit = "0123456789abcdef0123456789abcdef01234567";
        let valid_oid = "a".repeat(64);
        let entry =
            |kind: Option<&str>, path: &str, size: Option<u64>, lfs: Option<(&str, u64)>| {
                RootTreeEntry {
                    kind: kind.map(str::to_owned),
                    path: Some(path.into()),
                    size,
                    lfs: lfs.map(|(oid, size)| RootTreeLfs {
                        oid: Some(oid.into()),
                        size: Some(size),
                    }),
                }
            };
        let cases = [
            (
                entry(
                    Some("directory"),
                    "model.gguf",
                    Some(4),
                    Some((&valid_oid, 4)),
                ),
                UnsupportedPackagingReason::UnsupportedEntryType,
            ),
            (
                entry(
                    Some("file"),
                    "../model.gguf",
                    Some(4),
                    Some((&valid_oid, 4)),
                ),
                UnsupportedPackagingReason::UnsafePath,
            ),
            (
                entry(
                    Some("file"),
                    "nested/model.gguf",
                    Some(4),
                    Some((&valid_oid, 4)),
                ),
                UnsupportedPackagingReason::NestedPath,
            ),
            (
                entry(
                    Some("file"),
                    "model-00001-of-00002.gguf",
                    Some(4),
                    Some((&valid_oid, 4)),
                ),
                UnsupportedPackagingReason::Sharded,
            ),
            (
                entry(
                    Some("file"),
                    "model-draft.gguf",
                    Some(4),
                    Some((&valid_oid, 4)),
                ),
                UnsupportedPackagingReason::Auxiliary(AuxiliaryRole::Draft),
            ),
            (
                entry(Some("file"), "model.gguf", None, Some((&valid_oid, 4))),
                UnsupportedPackagingReason::MissingSize,
            ),
            (
                entry(Some("file"), "model.gguf", Some(0), Some((&valid_oid, 0))),
                UnsupportedPackagingReason::ZeroSize,
            ),
            (
                entry(Some("file"), "model.gguf", Some(4), None),
                UnsupportedPackagingReason::MissingLfsIdentity,
            ),
            (
                entry(Some("file"), "model.gguf", Some(4), Some((&valid_oid, 3))),
                UnsupportedPackagingReason::SizeMismatch,
            ),
            (
                entry(Some("file"), "model.gguf", Some(4), Some(("not-a-sha", 4))),
                UnsupportedPackagingReason::InvalidLfsSha256,
            ),
        ];

        for (entry, reason) in cases {
            assert_eq!(
                root_tree_candidate(entry, "owner/repo", commit).disposition(),
                CandidateDisposition::UnsupportedPackaging(reason)
            );
        }
    }

    #[test]
    fn composite_invalid_candidates_report_only_the_first_reason() {
        let commit = "0123456789abcdef0123456789abcdef01234567";
        let oid = "a".repeat(64);
        let candidate = |path: &str, size: Option<u64>, lfs: Option<(&str, u64)>| {
            root_tree_candidate(
                RootTreeEntry {
                    kind: Some("file".into()),
                    path: Some(path.into()),
                    size,
                    lfs: lfs.map(|(oid, size)| RootTreeLfs {
                        oid: Some(oid.into()),
                        size: Some(size),
                    }),
                },
                "owner/repo",
                commit,
            )
        };

        assert_eq!(
            candidate("../nested/model.gguf", None, None).disposition(),
            CandidateDisposition::UnsupportedPackaging(UnsupportedPackagingReason::UnsafePath)
        );
        assert_eq!(
            candidate("model-draft.gguf", Some(0), Some((&oid, 0))).disposition(),
            CandidateDisposition::UnsupportedPackaging(UnsupportedPackagingReason::Auxiliary(
                AuxiliaryRole::Draft,
            ))
        );
        assert_eq!(
            candidate("model.gguf", Some(4), Some(("invalid", 3))).disposition(),
            CandidateDisposition::UnsupportedPackaging(UnsupportedPackagingReason::SizeMismatch)
        );
    }

    #[test]
    fn tree_identity_accepts_only_oid_and_matching_positive_size() {
        let commit = "0123456789abcdef0123456789abcdef01234567";
        let uppercase_oid = "A".repeat(64);
        let candidate = root_tree_candidate(
            RootTreeEntry {
                kind: Some("file".into()),
                path: Some("model.gguf".into()),
                size: Some(42),
                lfs: Some(RootTreeLfs {
                    oid: Some(uppercase_oid),
                    size: Some(42),
                }),
            },
            "owner/repo",
            commit,
        );
        assert_eq!(
            candidate.disposition(),
            CandidateDisposition::EligibleForDownloadAndLocalValidation
        );
        let identity = candidate.identity().unwrap();
        assert_eq!(identity.repo(), "owner/repo");
        assert_eq!(identity.commit(), commit);
        assert_eq!(identity.path(), "model.gguf");
        assert_eq!(identity.sha256(), "a".repeat(64));
        assert_eq!(identity.size(), 42);

        for entry in [
            RootTreeEntry {
                kind: Some("file".into()),
                path: Some("model.gguf".into()),
                size: Some(4),
                lfs: None,
            },
            RootTreeEntry {
                kind: Some("file".into()),
                path: Some("model.gguf".into()),
                size: Some(4),
                lfs: Some(RootTreeLfs {
                    oid: None,
                    size: Some(4),
                }),
            },
            RootTreeEntry {
                kind: Some("file".into()),
                path: Some("model.gguf".into()),
                size: Some(4),
                lfs: Some(RootTreeLfs {
                    oid: Some("a".repeat(64)),
                    size: None,
                }),
            },
        ] {
            let candidate = root_tree_candidate(entry, "owner/repo", commit);
            assert_eq!(candidate.identity(), None);
            assert_eq!(
                candidate.disposition(),
                CandidateDisposition::UnsupportedPackaging(
                    UnsupportedPackagingReason::MissingLfsIdentity
                )
            );
        }

        for (entry, expected) in [
            (
                RootTreeEntry {
                    kind: Some("file".into()),
                    path: Some("model.gguf".into()),
                    size: Some(4),
                    lfs: Some(RootTreeLfs {
                        oid: Some("a".repeat(64)),
                        size: Some(0),
                    }),
                },
                UnsupportedPackagingReason::SizeMismatch,
            ),
            (
                RootTreeEntry {
                    kind: Some("file".into()),
                    path: Some("model.gguf".into()),
                    size: Some(4),
                    lfs: Some(RootTreeLfs {
                        oid: Some("a".repeat(64)),
                        size: Some(5),
                    }),
                },
                UnsupportedPackagingReason::SizeMismatch,
            ),
            (
                RootTreeEntry {
                    kind: Some("file".into()),
                    path: Some("model.gguf".into()),
                    size: Some(4),
                    lfs: Some(RootTreeLfs {
                        oid: Some("not-a-sha".into()),
                        size: Some(4),
                    }),
                },
                UnsupportedPackagingReason::InvalidLfsSha256,
            ),
        ] {
            let candidate = root_tree_candidate(entry, "owner/repo", commit);
            assert_eq!(candidate.identity(), None);
            assert_eq!(
                candidate.disposition(),
                CandidateDisposition::UnsupportedPackaging(expected)
            );
        }

        let tree_with_sha256_alias = format!(
            r#"[{{"type":"file","path":"model.gguf","size":4,"lfs":{{"sha256":"{}","size":4}}}}]"#,
            "a".repeat(64)
        );
        let mut transport = FakeDiscoveryTransport::queued(vec![
            FakeDiscoveryTransport::json_response(
                StatusCode::OK,
                &format!(r#"{{"sha":"{commit}"}}"#),
            ),
            FakeDiscoveryTransport::json_response(StatusCode::OK, &tree_with_sha256_alias),
        ]);
        let plan = inspect_repository_with_transport(
            InspectRepository::new("owner/repo".into(), None),
            &mut transport,
        )
        .unwrap();
        let alias_only = plan.candidates().first().expect("one GGUF candidate");
        assert_eq!(alias_only.identity(), None);
        assert_eq!(
            alias_only.disposition(),
            CandidateDisposition::UnsupportedPackaging(
                UnsupportedPackagingReason::MissingLfsIdentity
            )
        );
    }

    #[test]
    fn embedded_numeric_shard_markers_are_rejected_through_inspection() {
        let tree = serde_json::json!([
            {"type":"file","path":"model00001-of-00002.gguf","size":4,"lfs":{"oid":"a".repeat(64),"size":4}},
            {"type":"file","path":"model-00001-of-00002part.gguf","size":8,"lfs":{"oid":"b".repeat(64),"size":8}},
            {"type":"file","path":"model-of-something.gguf","size":16,"lfs":{"oid":"c".repeat(64),"size":16}},
            {"type":"file","path":"model-00001-of-words.gguf","size":32,"lfs":{"oid":"d".repeat(64),"size":32}},
            {"type":"file","path":"model-words-of-00002.gguf","size":64,"lfs":{"oid":"e".repeat(64),"size":64}}
        ])
        .to_string();
        let plan = inspected_plan(&tree);

        for path in ["model00001-of-00002.gguf", "model-00001-of-00002part.gguf"] {
            let candidate = plan
                .candidates()
                .iter()
                .find(|candidate| candidate.display_path() == path)
                .expect("shard marker candidate remains visible");
            assert_eq!(candidate.identity(), None, "{path}");
            assert_eq!(
                candidate.disposition(),
                CandidateDisposition::UnsupportedPackaging(UnsupportedPackagingReason::Sharded),
                "{path}"
            );
        }

        for path in [
            "model-of-something.gguf",
            "model-00001-of-words.gguf",
            "model-words-of-00002.gguf",
        ] {
            let candidate = plan
                .candidates()
                .iter()
                .find(|candidate| candidate.display_path() == path)
                .expect("ordinary nonnumeric name remains visible");
            assert_eq!(candidate.identity().map(ResolvedFile::path), Some(path));
            assert_eq!(
                candidate.disposition(),
                CandidateDisposition::EligibleForDownloadAndLocalValidation,
                "{path}"
            );
        }
    }

    #[test]
    fn numeric_shards_are_rejected_without_substring_false_positives() {
        assert!(numeric_shard_marker("model-00001-of-00002.gguf"));
        assert!(numeric_shard_marker("00001-of-00002-model.gguf"));
        assert!(numeric_shard_marker("model00001-of-00002.gguf"));
        assert!(numeric_shard_marker("model-00001-of-00002part.gguf"));

        assert!(!numeric_shard_marker("model-of-something.gguf"));
        assert!(!numeric_shard_marker("model-00001-of-words.gguf"));
        assert!(!numeric_shard_marker("model-words-of-00002.gguf"));
    }

    #[test]
    fn gguf_detection_handles_non_ascii_paths_without_panicking() {
        assert!(!is_gguf_like("ééé"));
        assert!(is_gguf_like("é-model.gguf"));
    }

    #[test]
    fn every_gguf_like_root_entry_remains_visible() {
        let commit = "0123456789abcdef0123456789abcdef01234567";
        let tree = serde_json::json!([
            {"type":"file","path":"eligible.GGUF","size":4,"lfs":{"oid":"a".repeat(64),"size":4}},
            {"type":"file","path":"second-eligible.gguf","size":8,"lfs":{"oid":"b".repeat(64),"size":8}},
            {"type":"directory","path":"directory.gguf","size":4,"lfs":{"oid":"c".repeat(64),"size":4}},
            {"type":"file","path":"nested/model.gguf","size":4,"lfs":{"oid":"d".repeat(64),"size":4}},
            {"type":"file","path":"model-00001-of-00002.gguf","size":4,"lfs":{"oid":"e".repeat(64),"size":4}},
            {"type":"file","path":"model\nunsafe.gguf","size":4,"lfs":{"oid":"f".repeat(64),"size":4}}
        ])
        .to_string();
        let mut transport = FakeDiscoveryTransport::queued(vec![
            FakeDiscoveryTransport::json_response(
                StatusCode::OK,
                &format!(r#"{{"sha":"{commit}"}}"#),
            ),
            FakeDiscoveryTransport::json_response(StatusCode::OK, &tree),
        ]);

        let plan = inspect_repository_with_transport(
            InspectRepository::new("owner/repo".into(), None),
            &mut transport,
        )
        .unwrap();
        let observed = plan
            .candidates()
            .iter()
            .map(|candidate| {
                (
                    candidate.display_path(),
                    candidate.identity().map(|identity| identity.path()),
                    candidate.disposition(),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(
            observed,
            vec![
                (
                    "eligible.GGUF",
                    Some("eligible.GGUF"),
                    CandidateDisposition::EligibleForDownloadAndLocalValidation,
                ),
                (
                    "second-eligible.gguf",
                    Some("second-eligible.gguf"),
                    CandidateDisposition::EligibleForDownloadAndLocalValidation,
                ),
                (
                    "directory.gguf",
                    None,
                    CandidateDisposition::UnsupportedPackaging(
                        UnsupportedPackagingReason::UnsupportedEntryType,
                    ),
                ),
                (
                    "nested/model.gguf",
                    None,
                    CandidateDisposition::UnsupportedPackaging(
                        UnsupportedPackagingReason::NestedPath,
                    ),
                ),
                (
                    "model-00001-of-00002.gguf",
                    None,
                    CandidateDisposition::UnsupportedPackaging(UnsupportedPackagingReason::Sharded),
                ),
                (
                    "<unsafe path>",
                    None,
                    CandidateDisposition::UnsupportedPackaging(
                        UnsupportedPackagingReason::UnsafePath,
                    ),
                ),
            ]
        );
        assert_eq!(transport.requests.len(), 2);
    }

    #[test]
    fn rejected_candidate_exposes_only_a_one_line_safe_display_path() {
        let candidate = root_tree_candidate(
            RootTreeEntry {
                kind: Some("file".into()),
                path: Some("model\nunsafe\tname.gguf".into()),
                size: Some(4),
                lfs: Some(RootTreeLfs {
                    oid: Some("a".repeat(64)),
                    size: Some(4),
                }),
            },
            "owner/repo",
            "0123456789abcdef0123456789abcdef01234567",
        );

        assert_eq!(candidate.display_path(), "<unsafe path>");
        assert!(candidate.identity().is_none());
        assert!(!format!("{candidate:?}").contains('\n'));
        assert!(!format!("{candidate:?}").contains('\t'));
    }

    #[test]
    fn inspection_sanitizes_unicode_terminal_controls_in_rejected_paths() {
        let commit = "0123456789abcdef0123456789abcdef01234567";
        let paths = [
            "model\u{2028}line.gguf",
            "model\u{2029}paragraph.gguf",
            "model\u{202e}bidi.gguf",
        ];
        let tree = serde_json::to_string(
            &paths
                .iter()
                .map(|path| {
                    serde_json::json!({
                        "type":"file",
                        "path":path,
                        "size":4,
                        "lfs":{"oid":"a".repeat(64),"size":4},
                    })
                })
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let mut transport = FakeDiscoveryTransport::queued(vec![
            FakeDiscoveryTransport::json_response(
                StatusCode::OK,
                &format!(r#"{{"sha":"{commit}"}}"#),
            ),
            FakeDiscoveryTransport::json_response(StatusCode::OK, &tree),
        ]);

        let plan = inspect_repository_with_transport(
            InspectRepository::new("owner/repo".into(), None),
            &mut transport,
        )
        .unwrap();
        let unsafe_characters = ['\u{2028}', '\u{2029}', '\u{202e}'];
        let observed = plan
            .candidates()
            .iter()
            .map(|candidate| {
                let debug = format!("{candidate:?}");
                (
                    candidate.disposition(),
                    candidate.identity().is_none(),
                    candidate.display_path() == "<unsafe path>",
                    unsafe_characters.iter().all(|character| {
                        !candidate.display_path().contains(*character)
                            && !debug.contains(*character)
                    }),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(
            observed,
            vec![
                (
                    CandidateDisposition::UnsupportedPackaging(
                        UnsupportedPackagingReason::UnsafePath,
                    ),
                    true,
                    true,
                    true,
                ),
                (
                    CandidateDisposition::UnsupportedPackaging(
                        UnsupportedPackagingReason::UnsafePath,
                    ),
                    true,
                    true,
                    true,
                ),
                (
                    CandidateDisposition::UnsupportedPackaging(
                        UnsupportedPackagingReason::UnsafePath,
                    ),
                    true,
                    true,
                    true,
                ),
            ]
        );
    }
}
