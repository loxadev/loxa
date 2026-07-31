use reqwest::blocking::{Client, RequestBuilder};
use reqwest::header::LINK;
use reqwest::{StatusCode, Url};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};

const HF_ORIGIN: &str = "https://huggingface.co";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedFile {
    pub repo: String,
    pub revision: String,
    pub filename: String,
    pub sha256: String,
    pub size: u64,
}

#[derive(Deserialize)]
struct ModelInfo {
    sha: String,
}

#[derive(Deserialize)]
struct TreeEntry {
    #[serde(rename = "type")]
    kind: String,
    path: String,
    size: u64,
    lfs: Option<Lfs>,
}

#[derive(Deserialize)]
struct Lfs {
    oid: String,
    size: u64,
}

pub fn parse_repo(input: &str) -> Result<String, String> {
    let repo = input.strip_prefix("hf://").unwrap_or(input);
    let parts = repo.split('/').collect::<Vec<_>>();
    if parts.len() == 2
        && parts
            .iter()
            .all(|part| !part.is_empty() && *part != "." && *part != "..")
    {
        Ok(repo.to_string())
    } else {
        Err("repository must be exactly owner/repo".into())
    }
}

pub fn resolve(
    client: &Client,
    repo: &str,
    revision: Option<&str>,
    filename: Option<&str>,
    quant: Option<&str>,
    token: Option<&str>,
) -> Result<ResolvedFile, String> {
    let repo = parse_repo(repo)?;
    let model_json = get_text(client, metadata_url(&repo, revision)?, token)?;
    let info: ModelInfo = serde_json::from_str(&model_json).map_err(|e| e.to_string())?;
    validate_hex(&info.sha, 40, "resolved revision")?;
    let mut next = Some(tree_url(&repo, &info.sha)?);
    let mut entries = Vec::<serde_json::Value>::new();
    while let Some(page_url) = next.take() {
        let response = authorized_request(client, page_url, token)
            .send()
            .map_err(|e| e.to_string())?;
        let response = status(response)?;
        next = response
            .headers()
            .get(LINK)
            .and_then(|value| value.to_str().ok())
            .and_then(next_link)
            .map(|value| pagination_url(&value))
            .transpose()?;
        let mut page: Vec<serde_json::Value> = response.json().map_err(|e| e.to_string())?;
        entries.append(&mut page);
    }
    select_from_json(
        &repo,
        filename,
        quant,
        &model_json,
        &serde_json::to_string(&entries).map_err(|e| e.to_string())?,
    )
}

fn metadata_url(repo: &str, revision: Option<&str>) -> Result<Url, String> {
    let (owner, name) = repo
        .split_once('/')
        .ok_or_else(|| "repository must be owner/repo".to_string())?;
    let mut url = Url::parse(HF_ORIGIN).map_err(|error| error.to_string())?;
    let mut path = url
        .path_segments_mut()
        .map_err(|_| "invalid Hugging Face origin")?;
    path.extend(["api", "models", owner, name]);
    if let Some(revision) = revision {
        path.extend(["revision", revision]);
    }
    drop(path);
    Ok(url)
}

fn tree_url(repo: &str, revision: &str) -> Result<Url, String> {
    let (owner, name) = repo
        .split_once('/')
        .ok_or_else(|| "repository must be owner/repo".to_string())?;
    let mut url = Url::parse(HF_ORIGIN).map_err(|error| error.to_string())?;
    url.path_segments_mut()
        .map_err(|_| "invalid Hugging Face origin")?
        .extend(["api", "models", owner, name, "tree", revision]);
    url.query_pairs_mut()
        .append_pair("recursive", "true")
        .append_pair("expand", "true");
    Ok(url)
}

fn pagination_url(value: &str) -> Result<Url, String> {
    let url = Url::parse(value).map_err(|error| error.to_string())?;
    if url.scheme() == "https"
        && url.host_str() == Some("huggingface.co")
        && url.port_or_known_default() == Some(443)
    {
        Ok(url)
    } else {
        Err("Hugging Face pagination changed origin".into())
    }
}

pub fn select_from_json(
    repo: &str,
    filename: Option<&str>,
    quant: Option<&str>,
    model_json: &str,
    tree_json: &str,
) -> Result<ResolvedFile, String> {
    let info: ModelInfo = serde_json::from_str(model_json).map_err(|e| e.to_string())?;
    validate_hex(&info.sha, 40, "resolved revision")?;
    let entries: Vec<TreeEntry> = serde_json::from_str(tree_json).map_err(|e| e.to_string())?;
    let mut candidates = entries
        .into_iter()
        .filter_map(|entry| {
            let lfs = entry.lfs?;
            let lower = entry.path.to_ascii_lowercase();
            let valid = entry.kind == "file"
                && lower.ends_with(".gguf")
                && !entry.path.contains(['/', '\\'])
                && !lower.contains("-of-")
                && entry.size == lfs.size
                && validate_hex(&lfs.oid, 64, "LFS SHA-256").is_ok();
            valid.then_some((entry.path, entry.size, lfs.oid.to_ascii_lowercase()))
        })
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Err("repository has no verified single-file GGUF".into());
    }
    if let Some(filename) = filename {
        candidates.retain(|candidate| candidate.0 == filename);
        if candidates.len() != 1 {
            return Err(format!("verified file {filename:?} not found"));
        }
    } else if let Some(quant) = quant {
        let matching = candidates
            .iter()
            .filter(|candidate| quantization(&candidate.0).eq_ignore_ascii_case(quant))
            .collect::<Vec<_>>();
        match matching.len() {
            0 => {
                let mut available = candidates
                    .iter()
                    .map(|candidate| quantization(&candidate.0))
                    .collect::<Vec<_>>();
                available.sort_unstable();
                available.dedup();
                return Err(format!(
                    "quantization {quant:?} is not available; available quantizations: {}. Retry with --quant <one of these values>.",
                    available.join(", ")
                ));
            }
            1 => candidates
                .retain(|candidate| quantization(&candidate.0).eq_ignore_ascii_case(quant)),
            _ => {
                let mut filenames = matching
                    .into_iter()
                    .map(|candidate| candidate.0.as_str())
                    .collect::<Vec<_>>();
                filenames.sort_unstable();
                return Err(format!(
                    "quantization {quant:?} matched multiple files: {}. Use --file <filename> to choose one.",
                    filenames.join(", ")
                ));
            }
        }
    } else {
        let q4 = candidates
            .iter()
            .filter(|candidate| quantization(&candidate.0).eq_ignore_ascii_case("Q4_K_M"))
            .count();
        if q4 == 1 {
            candidates
                .retain(|candidate| quantization(&candidate.0).eq_ignore_ascii_case("Q4_K_M"));
        } else if candidates.len() != 1 {
            return Err("GGUF selection is ambiguous; use --file or --quant".into());
        }
    }
    let (filename, size, sha256) = candidates.remove(0);
    Ok(ResolvedFile {
        repo: repo.into(),
        revision: info.sha,
        filename,
        sha256,
        size,
    })
}

pub fn discover_token() -> Option<String> {
    if std::env::var_os("HF_HUB_DISABLE_IMPLICIT_TOKEN").is_some_and(|value| !value.is_empty()) {
        return None;
    }
    if let Ok(token) = std::env::var("HF_TOKEN") {
        let token = token.trim();
        if !token.is_empty() {
            return Some(token.into());
        }
    }
    if let Some(path) = std::env::var_os("HF_TOKEN_PATH").map(PathBuf::from) {
        return read_token(&path);
    }
    let home = std::env::var_os("HF_HOME").map(PathBuf::from).or_else(|| {
        std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".cache/huggingface"))
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

fn get_text(client: &Client, url: Url, token: Option<&str>) -> Result<String, String> {
    let response = authorized_request(client, url, token)
        .send()
        .map_err(|e| e.to_string())?;
    status(response)?.text().map_err(|e| e.to_string())
}

fn status(response: reqwest::blocking::Response) -> Result<reqwest::blocking::Response, String> {
    match response.status() {
        StatusCode::UNAUTHORIZED => {
            Err("Hugging Face authentication token is missing or invalid".into())
        }
        StatusCode::FORBIDDEN => Err("Hugging Face access denied; check gated model access".into()),
        status if status.is_success() => Ok(response),
        status => Err(format!("Hugging Face returned HTTP {status}")),
    }
}

fn read_token(path: &Path) -> Option<String> {
    let value = fs::read_to_string(path).ok()?;
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}

fn validate_hex(value: &str, len: usize, label: &str) -> Result<(), String> {
    if value.len() == len && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(format!("invalid {label}"))
    }
}

fn quantization(filename: &str) -> &str {
    filename
        .strip_suffix(".gguf")
        .or_else(|| filename.strip_suffix(".GGUF"))
        .unwrap_or(filename)
        .rsplit(['-', '.'])
        .next()
        .unwrap_or("")
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
    use reqwest::header::AUTHORIZATION;

    const MODEL: &str = r#"{"sha":"0123456789abcdef0123456789abcdef01234567","gated":true}"#;
    const TREE: &str = r#"[
      {"type":"file","path":"demo-Q4_K_M.gguf","size":4,"lfs":{"oid":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","size":4}},
      {"type":"file","path":"demo-Q8_0.gguf","size":8,"lfs":{"oid":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","size":8}},
      {"type":"file","path":"split-00001-of-00002.gguf","size":2,"lfs":{"oid":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","size":2}}
    ]"#;

    #[test]
    fn pins_and_selects_without_silently_choosing_ambiguity() {
        let exact =
            select_from_json("owner/repo", Some("demo-Q8_0.gguf"), None, MODEL, TREE).unwrap();
        assert_eq!(exact.revision, "0123456789abcdef0123456789abcdef01234567");
        assert_eq!(exact.filename, "demo-Q8_0.gguf");

        let quant = select_from_json("owner/repo", None, Some("q8_0"), MODEL, TREE).unwrap();
        assert_eq!(quant.filename, "demo-Q8_0.gguf");
        let default = select_from_json("owner/repo", None, None, MODEL, TREE).unwrap();
        assert_eq!(default.filename, "demo-Q4_K_M.gguf");

        let ambiguous = TREE.replace("demo-Q4_K_M.gguf", "demo-Q5_K_M.gguf");
        assert!(
            select_from_json("owner/repo", None, None, MODEL, &ambiguous)
                .unwrap_err()
                .contains("ambiguous")
        );
    }

    #[test]
    fn missing_quant_lists_available_quantizations() {
        let error =
            select_from_json("owner/repo", None, Some("NOT_A_QUANT"), MODEL, TREE).unwrap_err();

        assert_eq!(
            error,
            "quantization \"NOT_A_QUANT\" is not available; available quantizations: Q4_K_M, Q8_0. Retry with --quant <one of these values>."
        );
    }

    #[test]
    fn ambiguous_quant_names_matching_files_and_directs_to_file() {
        let ambiguous = TREE.replace("demo-Q8_0.gguf", "other-Q4_K_M.gguf");
        let error =
            select_from_json("owner/repo", None, Some("Q4_K_M"), MODEL, &ambiguous).unwrap_err();

        assert_eq!(
            error,
            "quantization \"Q4_K_M\" matched multiple files: demo-Q4_K_M.gguf, other-Q4_K_M.gguf. Use --file <filename> to choose one."
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
    fn hf_urls_encode_segments_and_pagination_stays_on_exact_origin() {
        assert!(metadata_url("owner/repo", Some("refs/pr/1"))
            .unwrap()
            .as_str()
            .contains("/revision/refs%2Fpr%2F1"));
        assert!(
            tree_url("owner/repo", "0123456789abcdef0123456789abcdef01234567")
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
        assert!(pagination_url("https://evil.example/page").is_err());
    }
}
