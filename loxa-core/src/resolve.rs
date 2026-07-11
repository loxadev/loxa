use reqwest::blocking::Client;
use reqwest::header::LINK;
use serde::Deserialize;
use std::fmt;

const HF_API: &str = "https://huggingface.co/api/models";
const USER_AGENT: &str = concat!("loxa/", env!("CARGO_PKG_VERSION"));

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelReference {
    pub repo: String,
    pub revision: Option<String>,
    pub filename: Option<String>,
}

impl ModelReference {
    pub fn parse(input: &str) -> Result<Self, ResolveError> {
        let input = input.strip_prefix("hf://").unwrap_or(input);
        let (repo_and_revision, filename) = match input.split_once(':') {
            Some((left, right)) if !right.is_empty() => (left, Some(right.to_string())),
            Some(_) => return Err(ResolveError::InvalidReference),
            None => (input, None),
        };
        let (repo, revision) = match repo_and_revision.split_once('@') {
            Some((repo, revision)) if !revision.is_empty() => (repo, Some(revision.to_string())),
            Some(_) => return Err(ResolveError::InvalidReference),
            None => (repo_and_revision, None),
        };
        if repo.matches('/').count() != 1
            || repo
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == "..")
            || filename
                .as_deref()
                .is_some_and(|name| name.contains('/') || name.contains('\\'))
        {
            return Err(ResolveError::InvalidReference);
        }
        Ok(Self {
            repo: repo.to_string(),
            revision,
            filename,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedArtifact {
    pub repo: String,
    pub revision: String,
    pub filename: String,
    pub sha256: String,
    pub size_bytes: u64,
    pub license: String,
    pub quant: String,
    pub min_free_mem_gb: f32,
}

#[derive(Debug)]
pub enum ResolveError {
    InvalidReference,
    InvalidRevision,
    Gated,
    NoVerifiedGguf,
    QuantNotFound(String),
    NothingFits {
        available_bytes: u64,
        smallest_bytes: u64,
    },
    Http(String),
    InvalidResponse(String),
}

impl fmt::Display for ResolveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidReference => write!(f, "expected hf://owner/repo[@revision][:filename] or owner/repo"),
            Self::InvalidRevision => write!(f, "Hugging Face did not return a pinned commit hash"),
            Self::Gated => write!(f, "repository is gated; accept its terms and set HF_TOKEN before pulling"),
            Self::NoVerifiedGguf => write!(f, "repository has no GGUF artifact with an LFS SHA-256 identity"),
            Self::QuantNotFound(q) => write!(f, "repository has no verified GGUF matching --quant {q}"),
            Self::NothingFits { available_bytes, smallest_bytes } => write!(f, "no verified GGUF fits available RAM ({available_bytes} bytes); smallest requires about {} bytes", memory_bytes(*smallest_bytes)),
            Self::Http(message) | Self::InvalidResponse(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for ResolveError {}

#[derive(Deserialize)]
struct ModelInfo {
    sha: String,
    gated: serde_json::Value,
    #[serde(rename = "cardData", default)]
    card_data: CardData,
}

#[derive(Default, Deserialize)]
struct CardData {
    #[serde(default)]
    license: String,
}

#[derive(Deserialize)]
struct TreeEntry {
    #[serde(rename = "type")]
    kind: String,
    path: String,
    size: u64,
    lfs: Option<LfsInfo>,
}

#[derive(Deserialize)]
struct LfsInfo {
    oid: String,
    size: u64,
}

pub fn resolve(
    reference: &ModelReference,
    quant: Option<&str>,
    available_ram: u64,
) -> Result<ResolvedArtifact, ResolveError> {
    let client = Client::builder()
        .user_agent(USER_AGENT)
        .build()
        .map_err(|e| ResolveError::Http(e.to_string()))?;
    let model_url = match &reference.revision {
        Some(revision) => format!("{HF_API}/{}/revision/{revision}", reference.repo),
        None => format!("{HF_API}/{}", reference.repo),
    };
    let model = client
        .get(&model_url)
        .send()
        .and_then(|r| r.error_for_status())
        .map_err(|e| ResolveError::Http(e.to_string()))?
        .text()
        .map_err(|e| ResolveError::Http(e.to_string()))?;
    let info: ModelInfo =
        serde_json::from_str(&model).map_err(|e| ResolveError::InvalidResponse(e.to_string()))?;
    let tree_url = format!(
        "{HF_API}/{}/tree/{}?recursive=true&expand=false",
        reference.repo, info.sha
    );
    let mut next = Some(tree_url);
    let mut tree_entries = Vec::<serde_json::Value>::new();
    while let Some(url) = next.take() {
        let response = client
            .get(&url)
            .send()
            .and_then(|r| r.error_for_status())
            .map_err(|e| ResolveError::Http(e.to_string()))?;
        next = response
            .headers()
            .get(LINK)
            .and_then(|value| value.to_str().ok())
            .and_then(next_link);
        let body = response
            .text()
            .map_err(|e| ResolveError::Http(e.to_string()))?;
        let page: Vec<serde_json::Value> = serde_json::from_str(&body)
            .map_err(|e| ResolveError::InvalidResponse(e.to_string()))?;
        tree_entries.extend(page);
    }
    let tree = serde_json::to_string(&tree_entries)
        .map_err(|e| ResolveError::InvalidResponse(e.to_string()))?;
    resolve_from_json(
        &reference.repo,
        reference.filename.as_deref(),
        quant,
        available_ram,
        &model,
        &tree,
    )
}

pub fn resolve_from_json(
    repo: &str,
    filename: Option<&str>,
    quant: Option<&str>,
    available_ram: u64,
    model_json: &str,
    tree_json: &str,
) -> Result<ResolvedArtifact, ResolveError> {
    let model: ModelInfo = serde_json::from_str(model_json)
        .map_err(|e| ResolveError::InvalidResponse(e.to_string()))?;
    if model.gated != serde_json::Value::Bool(false) {
        return Err(ResolveError::Gated);
    }
    if model.card_data.license.trim().is_empty() {
        return Err(ResolveError::InvalidResponse(
            "model cardData is missing a license".into(),
        ));
    }
    if model.sha.len() != 40 || !model.sha.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(ResolveError::InvalidRevision);
    }
    let entries: Vec<TreeEntry> = serde_json::from_str(tree_json)
        .map_err(|e| ResolveError::InvalidResponse(e.to_string()))?;
    let mut candidates = entries
        .into_iter()
        .filter_map(|entry| {
            let TreeEntry {
                kind,
                path,
                size,
                lfs,
            } = entry;
            let lfs = lfs?;
            let is_gguf = kind == "file" && path.to_ascii_lowercase().ends_with(".gguf");
            let flat = !path.contains('/') && !path.contains('\\');
            let valid_oid = lfs.oid.len() == 64 && lfs.oid.bytes().all(|b| b.is_ascii_hexdigit());
            (is_gguf && flat && valid_oid && size == lfs.size).then_some({
                (
                    TreeEntry {
                        kind,
                        path,
                        size,
                        lfs: None,
                    },
                    lfs,
                )
            })
        })
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Err(ResolveError::NoVerifiedGguf);
    }
    if let Some(filename) = filename {
        candidates.retain(|(entry, _)| entry.path == filename);
    }
    if let Some(quant) = quant {
        candidates
            .retain(|(entry, _)| quant_from_filename(&entry.path).eq_ignore_ascii_case(quant));
    }
    if candidates.is_empty() {
        return Err(ResolveError::QuantNotFound(
            quant
                .or(filename)
                .unwrap_or("requested artifact")
                .to_string(),
        ));
    }
    candidates.sort_by_key(|(entry, _)| entry.size);
    let smallest = candidates[0].0.size;
    let (entry, lfs) = candidates
        .into_iter()
        .filter(|(entry, _)| memory_bytes(entry.size) <= available_ram)
        .max_by_key(|(entry, _)| entry.size)
        .ok_or(ResolveError::NothingFits {
            available_bytes: available_ram,
            smallest_bytes: smallest,
        })?;
    Ok(ResolvedArtifact {
        repo: repo.to_string(),
        revision: model.sha,
        filename: entry.path.clone(),
        sha256: lfs.oid.to_ascii_lowercase(),
        size_bytes: entry.size,
        license: model.card_data.license,
        quant: quant_from_filename(&entry.path),
        min_free_mem_gb: ((entry.size as f64 / 1024_f64.powi(3)) * 1.15 * 10.0).round() as f32
            / 10.0,
    })
}

fn memory_bytes(size: u64) -> u64 {
    ((size as f64) * 1.15).ceil() as u64
}

fn quant_from_filename(filename: &str) -> String {
    let stem = filename
        .strip_suffix(".gguf")
        .or_else(|| filename.strip_suffix(".GGUF"))
        .unwrap_or(filename);
    stem.rsplit(['-', '.'])
        .next()
        .unwrap_or("unknown")
        .to_ascii_uppercase()
}

fn next_link(header: &str) -> Option<String> {
    header.split(',').find_map(|part| {
        let (url, attributes) = part.trim().split_once('>')?;
        attributes
            .contains("rel=\"next\"")
            .then(|| url.strip_prefix('<').unwrap_or(url).to_string())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const MODEL: &str = r#"{
      "id":"acme/demo-GGUF", "sha":"0123456789abcdef0123456789abcdef01234567",
      "gated":false, "cardData":{"license":"apache-2.0"}
    }"#;
    const TREE: &str = r#"[
      {"type":"file","path":"demo-Q8_0.gguf","size":8000,"lfs":{"oid":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","size":8000}},
      {"type":"file","path":"demo-Q4_K_M.gguf","size":4000,"lfs":{"oid":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","size":4000}},
      {"type":"file","path":"unverified-Q2_K.gguf","size":2000}
    ]"#;

    #[test]
    fn parses_supported_hf_references() {
        assert_eq!(
            ModelReference::parse("hf://owner/repo@dev:file.gguf").unwrap(),
            ModelReference {
                repo: "owner/repo".into(),
                revision: Some("dev".into()),
                filename: Some("file.gguf".into())
            }
        );
        assert_eq!(
            ModelReference::parse("owner/repo").unwrap().repo,
            "owner/repo"
        );
        assert!(ModelReference::parse("owner").is_err());
    }

    #[test]
    fn resolves_pinned_lfs_candidate_that_fits_ram() {
        let resolved = resolve_from_json("owner/repo", None, None, 5_000, MODEL, TREE).unwrap();
        assert_eq!(
            resolved.revision,
            "0123456789abcdef0123456789abcdef01234567"
        );
        assert_eq!(resolved.filename, "demo-Q4_K_M.gguf");
        assert_eq!(
            resolved.sha256,
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );
    }

    #[test]
    fn manual_quant_override_is_exact_and_still_ram_checked() {
        let resolved =
            resolve_from_json("owner/repo", None, Some("Q8_0"), 10_000, MODEL, TREE).unwrap();
        assert_eq!(resolved.quant, "Q8_0");
        assert!(matches!(
            resolve_from_json("owner/repo", None, Some("Q8_0"), 8_000, MODEL, TREE),
            Err(ResolveError::NothingFits { .. })
        ));
    }

    #[test]
    fn rejects_gated_no_gguf_and_unpinned_repositories() {
        let gated = MODEL.replace("\"gated\":false", "\"gated\":true");
        assert!(matches!(
            resolve_from_json("owner/repo", None, None, 10_000, &gated, TREE),
            Err(ResolveError::Gated)
        ));
        assert!(matches!(
            resolve_from_json("owner/repo", None, None, 10_000, MODEL, "[]"),
            Err(ResolveError::NoVerifiedGguf)
        ));
        let unpinned = MODEL.replace("0123456789abcdef0123456789abcdef01234567", "main");
        assert!(matches!(
            resolve_from_json("owner/repo", None, None, 10_000, &unpinned, TREE),
            Err(ResolveError::InvalidRevision)
        ));
    }

    #[test]
    fn extracts_unlisted_quantization_names_and_next_page_links() {
        assert_eq!(quant_from_filename("demo-UD-Q4_K_XL.gguf"), "Q4_K_XL");
        assert_eq!(quant_from_filename("demo.Q2_K.gguf"), "Q2_K");
        assert_eq!(
            next_link("<https://huggingface.co/api/tree?cursor=2>; rel=\"next\""),
            Some("https://huggingface.co/api/tree?cursor=2".into())
        );
    }

    #[test]
    #[ignore = "hits Hugging Face; run manually when validating live model intake"]
    fn live_resolver_finds_a_real_tiny_gguf_repository() {
        let reference =
            ModelReference::parse("hf://TinyLlama/TinyLlama-1.1B-Chat-v0.2-GGUF").unwrap();
        let artifact = resolve(&reference, None, u64::MAX).unwrap();
        assert_eq!(artifact.revision.len(), 40);
        assert_eq!(artifact.sha256.len(), 64);
        assert!(artifact.filename.ends_with(".gguf"));
    }
}
