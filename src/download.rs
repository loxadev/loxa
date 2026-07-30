use crate::huggingface::{authorized_request, ResolvedFile};
use reqwest::blocking::Client;
use reqwest::header::{CONTENT_RANGE, LOCATION, RANGE};
use reqwest::{redirect::Policy, StatusCode, Url};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

const MAX_REDIRECTS: usize = 5;

pub struct Transfer {
    pub status: StatusCode,
    pub content_range: Option<String>,
    pub reader: Box<dyn Read>,
}

pub trait Transport {
    fn get(&self, url: &Url, offset: Option<u64>) -> Result<Transfer, String>;
}

pub struct ReqwestTransport {
    client: Client,
    token: Option<String>,
}

impl ReqwestTransport {
    pub fn new(token: Option<String>) -> Result<Self, String> {
        let client = Client::builder()
            .user_agent(concat!("loxa/", env!("CARGO_PKG_VERSION")))
            .connect_timeout(Duration::from_secs(30))
            .timeout(None)
            .redirect(Policy::none())
            .build()
            .map_err(|error| error.to_string())?;
        Ok(Self { client, token })
    }
}

impl Transport for ReqwestTransport {
    fn get(&self, url: &Url, offset: Option<u64>) -> Result<Transfer, String> {
        let mut current = url.clone();
        for redirect in 0..=MAX_REDIRECTS {
            let mut request =
                authorized_request(&self.client, current.clone(), self.token.as_deref());
            if let Some(offset) = offset {
                request = request.header(RANGE, format!("bytes={offset}-"));
            }
            let response = request
                .send()
                .map_err(|_| "artifact request failed".to_string())?;
            if response.status().is_redirection() {
                if redirect == MAX_REDIRECTS {
                    return Err("too many artifact redirects".into());
                }
                let location = response
                    .headers()
                    .get(LOCATION)
                    .ok_or_else(|| "artifact redirect omitted Location".to_string())?
                    .to_str()
                    .map_err(|_| "invalid artifact redirect".to_string())?;
                current = current
                    .join(location)
                    .map_err(|_| "invalid artifact redirect".to_string())?;
                continue;
            }
            match response.status() {
                StatusCode::UNAUTHORIZED => {
                    return Err("Hugging Face authentication token is missing or invalid".into())
                }
                StatusCode::FORBIDDEN => {
                    return Err("Hugging Face access denied; check gated model access".into())
                }
                status if status.is_success() => {
                    let content_range = response
                        .headers()
                        .get(CONTENT_RANGE)
                        .map(|value| value.to_str().map(str::to_string))
                        .transpose()
                        .map_err(|_| "invalid Content-Range".to_string())?;
                    return Ok(Transfer {
                        status,
                        content_range,
                        reader: Box::new(response),
                    });
                }
                status => return Err(format!("artifact server returned HTTP {status}")),
            }
        }
        Err("too many artifact redirects".into())
    }
}

pub fn artifact_url(file: &ResolvedFile) -> Result<Url, String> {
    let (owner, repo) = file
        .repo
        .split_once('/')
        .ok_or_else(|| "repository must be owner/repo".to_string())?;
    let mut url = Url::parse("https://huggingface.co").map_err(|error| error.to_string())?;
    url.path_segments_mut()
        .map_err(|_| "invalid Hugging Face origin")?
        .extend([owner, repo, "resolve", &file.revision, &file.filename]);
    Ok(url)
}

pub fn download(
    file: &ResolvedFile,
    model_dir: &Path,
    token: Option<String>,
) -> Result<PathBuf, String> {
    let transport = ReqwestTransport::new(token)?;
    download_with_transport(file, model_dir, &transport)
}

pub fn download_with_transport(
    spec: &ResolvedFile,
    model_dir: &Path,
    transport: &impl Transport,
) -> Result<PathBuf, String> {
    fs::create_dir_all(model_dir).map_err(|error| error.to_string())?;
    if !fs::symlink_metadata(model_dir)
        .map_err(|error| error.to_string())?
        .file_type()
        .is_dir()
    {
        return Err(format!("unsafe model directory {}", model_dir.display()));
    }
    let final_path = model_dir.join("model.gguf");
    let part_path = model_dir.join("model.gguf.part");
    let restart_path = model_dir.join("model.gguf.part.restart");
    let invalid_path = model_dir.join("model.gguf.invalid");
    if final_path.exists() {
        reject_non_regular_if_present(&final_path)?;
        if verify_regular(&final_path, spec.size, &spec.sha256).is_ok() {
            return Ok(final_path);
        }
        if invalid_path.exists() {
            return Err("a prior corrupt artifact repair is still pending".into());
        }
        fs::rename(&final_path, &invalid_path).map_err(|error| error.to_string())?;
    }
    reject_unsafe_transfer_if_present(&part_path)?;
    reject_unsafe_transfer_if_present(&restart_path)?;
    reject_non_regular_if_present(&invalid_path)?;
    let mut offset = fs::metadata(&part_path).map(|meta| meta.len()).unwrap_or(0);
    if offset > spec.size {
        fs::remove_file(&part_path).map_err(|error| error.to_string())?;
        offset = 0;
    }
    if offset == spec.size && offset > 0 {
        sync_transfer_file(&part_path)?;
        if let Err(error) = verify_regular(&part_path, spec.size, &spec.sha256) {
            fs::remove_file(&part_path).map_err(|remove| remove.to_string())?;
            return Err(error);
        }
        fs::rename(&part_path, &final_path).map_err(|error| error.to_string())?;
        finish_repair(model_dir, &invalid_path, &restart_path)?;
        return Ok(final_path);
    }
    let mut transfer = transport.get(&artifact_url(spec)?, (offset > 0).then_some(offset))?;
    let ignored_range = offset > 0 && transfer.status == StatusCode::OK;
    let (target, append) = if ignored_range {
        (&restart_path, false)
    } else {
        if transfer.status == StatusCode::PARTIAL_CONTENT {
            validate_content_range(transfer.content_range.as_deref(), offset, spec.size)?;
        } else if transfer.status != StatusCode::OK || offset > 0 {
            return Err(format!("unexpected artifact HTTP {}", transfer.status));
        }
        (&part_path, offset > 0)
    };
    let expected_written = if append {
        spec.size - offset
    } else {
        spec.size
    };
    let read_limit = expected_written
        .checked_add(1)
        .ok_or_else(|| "artifact is too large".to_string())?;
    let mut options = OpenOptions::new();
    options
        .create(true)
        .write(true)
        .append(append)
        .truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let mut output = options
        .open(target)
        .map_err(|error| format!("{}: {error}", target.display()))?;
    reject_unsafe_open_transfer(&output, target)?;
    if !append {
        output
            .set_len(0)
            .map_err(|error| format!("{}: {error}", target.display()))?;
    }
    let copied = copy_bounded(transfer.reader.as_mut(), &mut output, target, read_limit)?;
    if copied > expected_written {
        drop(output);
        fs::remove_file(target).map_err(|error| format!("{}: {error}", target.display()))?;
        return Err(format!(
            "artifact size mismatch: expected {expected_written} downloaded bytes, got more"
        ));
    }
    output
        .sync_all()
        .map_err(|error| format!("{}: {error}", target.display()))?;
    if copied != expected_written {
        return Err(format!(
            "artifact size mismatch: expected {expected_written} downloaded bytes, got {copied}"
        ));
    }
    if let Err(error) = verify_regular(target, spec.size, &spec.sha256) {
        fs::remove_file(target).map_err(|remove| remove.to_string())?;
        return Err(error);
    }
    if ignored_range {
        fs::remove_file(&part_path).map_err(|error| error.to_string())?;
        fs::rename(&restart_path, &part_path).map_err(|error| error.to_string())?;
    }
    fs::rename(&part_path, &final_path).map_err(|error| error.to_string())?;
    finish_repair(model_dir, &invalid_path, &restart_path)?;
    Ok(final_path)
}

fn copy_bounded(
    reader: &mut dyn Read,
    output: &mut File,
    target: &Path,
    limit: u64,
) -> Result<u64, String> {
    let mut copied = 0;
    let mut buffer = [0_u8; 64 * 1024];
    while copied < limit {
        let remaining = (limit - copied).min(buffer.len() as u64) as usize;
        let read = match reader.read(&mut buffer[..remaining]) {
            Ok(read) => read,
            Err(_) => {
                output
                    .sync_all()
                    .map_err(|error| format!("{}: {error}", target.display()))?;
                return Err("artifact response body failed".into());
            }
        };
        if read == 0 {
            break;
        }
        output
            .write_all(&buffer[..read])
            .map_err(|error| format!("{}: {error}", target.display()))?;
        copied += read as u64;
    }
    Ok(copied)
}

fn sync_transfer_file(path: &Path) -> Result<(), String> {
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options
        .open(path)
        .map_err(|error| format!("{}: {error}", path.display()))?;
    reject_unsafe_open_transfer(&file, path)?;
    file.sync_all()
        .map_err(|error| format!("{}: {error}", path.display()))
}

fn finish_repair(model_dir: &Path, invalid_path: &Path, restart_path: &Path) -> Result<(), String> {
    for path in [invalid_path, restart_path] {
        if path.exists() {
            fs::remove_file(path).map_err(|error| error.to_string())?;
        }
    }
    File::open(model_dir)
        .and_then(|dir| dir.sync_all())
        .map_err(|error| error.to_string())?;
    Ok(())
}

pub fn verify_regular(path: &Path, size: u64, sha256: &str) -> Result<(), String> {
    let metadata = fs::symlink_metadata(path).map_err(|error| error.to_string())?;
    if !metadata.file_type().is_file() || metadata.len() != size {
        return Err(format!("invalid model artifact {}", path.display()));
    }
    let mut file = File::open(path).map_err(|error| error.to_string())?;
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(|error| error.to_string())?;
        if read == 0 {
            break;
        }
        hash.update(&buffer[..read]);
    }
    let actual = hex(hash.finalize().as_ref());
    if actual != sha256.to_ascii_lowercase() {
        return Err("model artifact checksum mismatch".into());
    }
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(DIGITS[(byte >> 4) as usize] as char);
        encoded.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn reject_non_regular_if_present(path: &Path) -> Result<(), String> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(()),
        Ok(_) => Err(format!("unsafe artifact path {}", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.to_string()),
    }
}

fn reject_unsafe_transfer_if_present(path: &Path) -> Result<(), String> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => {
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                if metadata.nlink() != 1 {
                    return Err(format!("unsafe artifact path {}", path.display()));
                }
            }
            Ok(())
        }
        Ok(_) => Err(format!("unsafe artifact path {}", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.to_string()),
    }
}

fn reject_unsafe_open_transfer(file: &File, path: &Path) -> Result<(), String> {
    let metadata = file
        .metadata()
        .map_err(|error| format!("{}: {error}", path.display()))?;
    if !metadata.file_type().is_file() {
        return Err(format!("unsafe artifact path {}", path.display()));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(format!("unsafe artifact path {}", path.display()));
        }
    }
    Ok(())
}

fn validate_content_range(value: Option<&str>, offset: u64, total: u64) -> Result<(), String> {
    let value = value.ok_or_else(|| "206 response omitted Content-Range".to_string())?;
    let rest = value
        .strip_prefix("bytes ")
        .ok_or_else(|| "invalid Content-Range".to_string())?;
    let (range, observed_total) = rest
        .split_once('/')
        .ok_or_else(|| "invalid Content-Range".to_string())?;
    let (start, end) = range
        .split_once('-')
        .ok_or_else(|| "invalid Content-Range".to_string())?;
    let start = start.parse::<u64>().map_err(|_| "invalid Content-Range")?;
    let end = end.parse::<u64>().map_err(|_| "invalid Content-Range")?;
    let observed_total = observed_total
        .parse::<u64>()
        .map_err(|_| "invalid Content-Range")?;
    if start == offset && observed_total == total && end.checked_add(1) == Some(total) {
        Ok(())
    } else {
        Err("Content-Range does not match requested artifact".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::huggingface::ResolvedFile;
    use sha2::{Digest, Sha256};
    use std::cell::{Cell, RefCell};
    use std::io::{Cursor, Error, Write};
    use std::rc::Rc;
    use tempfile::tempdir;

    struct FakeTransport {
        responses: RefCell<Vec<Transfer>>,
        offsets: RefCell<Vec<Option<u64>>>,
    }

    struct ReaderThatFailsAfterPrefix {
        prefix: Cursor<Vec<u8>>,
    }

    struct FiniteLargeReader {
        remaining: usize,
        consumed: Rc<Cell<usize>>,
    }

    impl Read for ReaderThatFailsAfterPrefix {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            let read = self.prefix.read(buffer)?;
            if read > 0 {
                Ok(read)
            } else {
                Err(Error::other("simulated reader failure"))
            }
        }
    }

    impl Read for FiniteLargeReader {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            let read = self.remaining.min(buffer.len());
            buffer[..read].fill(b'x');
            self.remaining -= read;
            self.consumed.set(self.consumed.get() + read);
            Ok(read)
        }
    }

    impl Transport for FakeTransport {
        fn get(&self, _url: &Url, offset: Option<u64>) -> Result<Transfer, String> {
            self.offsets.borrow_mut().push(offset);
            Ok(self.responses.borrow_mut().remove(0))
        }
    }

    fn transfer(status: StatusCode, range: Option<&str>, bytes: &[u8]) -> Transfer {
        Transfer {
            status,
            content_range: range.map(str::to_string),
            reader: Box::new(Cursor::new(bytes.to_vec())),
        }
    }

    fn failing_transfer(status: StatusCode, range: Option<&str>, prefix: &[u8]) -> Transfer {
        Transfer {
            status,
            content_range: range.map(str::to_string),
            reader: Box::new(ReaderThatFailsAfterPrefix {
                prefix: Cursor::new(prefix.to_vec()),
            }),
        }
    }

    fn spec(bytes: &[u8]) -> ResolvedFile {
        ResolvedFile {
            repo: "owner/repo".into(),
            revision: "0123456789abcdef0123456789abcdef01234567".into(),
            filename: "model.gguf".into(),
            sha256: hex(Sha256::digest(bytes).as_ref()),
            size: bytes.len() as u64,
        }
    }

    #[test]
    fn fresh_resume_ignored_range_and_checksum_are_verified_before_promotion() {
        let bytes = b"abcdef";

        let fresh_dir = tempdir().unwrap();
        let fresh = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, bytes)]),
            offsets: RefCell::new(Vec::new()),
        };
        let path = download_with_transport(&spec(bytes), fresh_dir.path(), &fresh).unwrap();
        assert_eq!(std::fs::read(path).unwrap(), bytes);
        assert_eq!(&*fresh.offsets.borrow(), &[None]);

        let resume_dir = tempdir().unwrap();
        std::fs::write(resume_dir.path().join("model.gguf.part"), b"abc").unwrap();
        let resume = FakeTransport {
            responses: RefCell::new(vec![transfer(
                StatusCode::PARTIAL_CONTENT,
                Some("bytes 3-5/6"),
                b"def",
            )]),
            offsets: RefCell::new(Vec::new()),
        };
        download_with_transport(&spec(bytes), resume_dir.path(), &resume).unwrap();
        assert_eq!(&*resume.offsets.borrow(), &[Some(3)]);

        let restart_dir = tempdir().unwrap();
        std::fs::write(restart_dir.path().join("model.gguf.part"), b"bad").unwrap();
        let restart = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, bytes)]),
            offsets: RefCell::new(Vec::new()),
        };
        download_with_transport(&spec(bytes), restart_dir.path(), &restart).unwrap();
        assert_eq!(
            std::fs::read(restart_dir.path().join("model.gguf")).unwrap(),
            bytes
        );

        let bad_dir = tempdir().unwrap();
        let bad = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, b"abcdeg")]),
            offsets: RefCell::new(Vec::new()),
        };
        assert!(download_with_transport(&spec(bytes), bad_dir.path(), &bad)
            .unwrap_err()
            .contains("checksum"));
        assert!(!bad_dir.path().join("model.gguf").exists());
    }

    #[test]
    fn corrupt_regular_final_is_preserved_until_repair_is_verified() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("model.gguf"), b"corrupt").unwrap();
        let transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, b"abcdef")]),
            offsets: RefCell::new(Vec::new()),
        };

        download_with_transport(&spec(b"abcdef"), dir.path(), &transport).unwrap();

        assert_eq!(
            std::fs::read(dir.path().join("model.gguf")).unwrap(),
            b"abcdef"
        );
        assert!(!dir.path().join("model.gguf.invalid").exists());
    }

    #[test]
    fn corrupt_complete_partial_is_cleared_for_the_next_pull() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("model.gguf.part"), b"abcdeg").unwrap();
        let first = FakeTransport {
            responses: RefCell::new(Vec::new()),
            offsets: RefCell::new(Vec::new()),
        };
        assert!(download_with_transport(&spec(b"abcdef"), dir.path(), &first).is_err());
        let second = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, b"abcdef")]),
            offsets: RefCell::new(Vec::new()),
        };
        download_with_transport(&spec(b"abcdef"), dir.path(), &second).unwrap();
        assert_eq!(
            std::fs::read(dir.path().join("model.gguf")).unwrap(),
            b"abcdef"
        );
    }

    #[test]
    fn reader_failure_preserves_prefix_for_an_exact_range_resume() {
        let bytes = b"abcdef";
        let dir = tempdir().unwrap();
        let interrupted = FakeTransport {
            responses: RefCell::new(vec![failing_transfer(StatusCode::OK, None, b"abc")]),
            offsets: RefCell::new(Vec::new()),
        };

        assert!(download_with_transport(&spec(bytes), dir.path(), &interrupted).is_err());
        assert!(!dir.path().join("model.gguf").exists());
        assert_eq!(
            std::fs::read(dir.path().join("model.gguf.part")).unwrap(),
            b"abc"
        );

        let resumed = FakeTransport {
            responses: RefCell::new(vec![transfer(
                StatusCode::PARTIAL_CONTENT,
                Some("bytes 3-5/6"),
                b"def",
            )]),
            offsets: RefCell::new(Vec::new()),
        };
        let final_path = download_with_transport(&spec(bytes), dir.path(), &resumed).unwrap();

        assert_eq!(std::fs::read(final_path).unwrap(), bytes);
        assert_eq!(&*resumed.offsets.borrow(), &[Some(3)]);
    }

    #[test]
    fn mismatched_content_range_leaves_existing_partial_unchanged() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("model.gguf.part"), b"abc").unwrap();
        let transport = FakeTransport {
            responses: RefCell::new(vec![transfer(
                StatusCode::PARTIAL_CONTENT,
                Some("bytes 2-5/6"),
                b"def",
            )]),
            offsets: RefCell::new(Vec::new()),
        };

        let error = download_with_transport(&spec(b"abcdef"), dir.path(), &transport).unwrap_err();

        assert!(error.contains("Content-Range"));
        assert_eq!(
            std::fs::read(dir.path().join("model.gguf.part")).unwrap(),
            b"abc"
        );
        assert!(!dir.path().join("model.gguf").exists());
        assert_eq!(&*transport.offsets.borrow(), &[Some(3)]);
    }

    #[test]
    fn checksum_invalid_completed_transfer_is_removed_before_next_pull() {
        let dir = tempdir().unwrap();
        let transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, b"abcdeg")]),
            offsets: RefCell::new(Vec::new()),
        };

        let error = download_with_transport(&spec(b"abcdef"), dir.path(), &transport).unwrap_err();

        assert!(error.contains("checksum"));
        assert!(!dir.path().join("model.gguf").exists());
        assert!(!dir.path().join("model.gguf.part").exists());
    }

    #[test]
    fn oversized_partial_is_discarded_before_a_zero_offset_restart() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("model.gguf.part"), b"abcdefg").unwrap();
        let transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, b"abcdef")]),
            offsets: RefCell::new(Vec::new()),
        };

        let final_path = download_with_transport(&spec(b"abcdef"), dir.path(), &transport).unwrap();

        assert_eq!(std::fs::read(final_path).unwrap(), b"abcdef");
        assert!(!dir.path().join("model.gguf.part").exists());
        assert_eq!(&*transport.offsets.borrow(), &[None]);
    }

    #[test]
    fn oversized_response_is_bounded_and_does_not_leave_a_transfer_target() {
        let dir = tempdir().unwrap();
        let consumed = Rc::new(Cell::new(0));
        let transport = FakeTransport {
            responses: RefCell::new(vec![Transfer {
                status: StatusCode::OK,
                content_range: None,
                reader: Box::new(FiniteLargeReader {
                    remaining: 64,
                    consumed: Rc::clone(&consumed),
                }),
            }]),
            offsets: RefCell::new(Vec::new()),
        };

        assert!(download_with_transport(&spec(b"abcdef"), dir.path(), &transport).is_err());

        assert_eq!(consumed.get(), 7);
        assert!(!dir.path().join("model.gguf").exists());
        assert!(!dir.path().join("model.gguf.part").exists());
        assert!(!dir.path().join("model.gguf.part.restart").exists());
    }

    #[cfg(unix)]
    #[test]
    fn hard_linked_partial_is_rejected_before_mutation() {
        let dir = tempdir().unwrap();
        let witness = dir.path().join("witness");
        let partial = dir.path().join("model.gguf.part");
        std::fs::write(&witness, b"abc").unwrap();
        std::fs::hard_link(&witness, &partial).unwrap();
        let transport = FakeTransport {
            responses: RefCell::new(vec![transfer(
                StatusCode::PARTIAL_CONTENT,
                Some("bytes 3-5/6"),
                b"def",
            )]),
            offsets: RefCell::new(Vec::new()),
        };

        assert!(download_with_transport(&spec(b"abcdef"), dir.path(), &transport).is_err());

        assert_eq!(std::fs::read(&witness).unwrap(), b"abc");
        assert_eq!(std::fs::read(&partial).unwrap(), b"abc");
        assert!(!dir.path().join("model.gguf").exists());
        assert!(transport.offsets.borrow().is_empty());
    }

    #[test]
    fn redirect_request_error_does_not_expose_query_credentials() {
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut first, _) = listener.accept().unwrap();
            let mut request = [0_u8; 1];
            first.read_exact(&mut request).unwrap();
            first
                .write_all(
                    b"HTTP/1.1 302 Found\r\nLocation: /artifact?X-Amz-Credential=secret-token\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
            drop(first);

            let (mut redirected, _) = listener.accept().unwrap();
            redirected.read_exact(&mut request).unwrap();
        });
        let transport = ReqwestTransport::new(None).unwrap();
        let url = Url::parse(&format!("http://{address}/start")).unwrap();

        let error = match transport.get(&url, None) {
            Ok(_) => panic!("the redirected request must fail"),
            Err(error) => error,
        };
        server.join().unwrap();

        assert!(!error.contains('?'), "{error}");
        assert!(!error.contains("X-Amz-Credential"), "{error}");
        assert!(!error.contains("secret-token"), "{error}");
    }

    #[test]
    fn successful_ranged_resume_removes_stale_restart_file() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("model.gguf.part"), b"abc").unwrap();
        std::fs::write(dir.path().join("model.gguf.part.restart"), b"stale").unwrap();
        let transport = FakeTransport {
            responses: RefCell::new(vec![transfer(
                StatusCode::PARTIAL_CONTENT,
                Some("bytes 3-5/6"),
                b"def",
            )]),
            offsets: RefCell::new(Vec::new()),
        };

        let final_path = download_with_transport(&spec(b"abcdef"), dir.path(), &transport).unwrap();

        assert_eq!(std::fs::read(final_path).unwrap(), b"abcdef");
        assert!(!dir.path().join("model.gguf.part.restart").exists());
    }
}
