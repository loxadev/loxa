use crate::huggingface::{should_attach_token, ResolvedFile};
use reqwest::header::{CONTENT_RANGE, LOCATION, RANGE};
use reqwest::{redirect::Policy, StatusCode, Url};
use std::fmt::{self, Display};
use std::io::{self, Read};
use std::rc::Rc;
use std::time::Duration;

const MAX_REDIRECTS: usize = 5;
const READ_IDLE_TIMEOUT: Duration = Duration::from_secs(120);

pub(super) struct Transfer {
    pub(super) status: StatusCode,
    pub(super) content_range: Option<String>,
    pub(super) reader: Box<dyn Read>,
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct TransferError {
    message: String,
    retryable: bool,
}

impl TransferError {
    pub(super) fn retryable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: true,
        }
    }

    pub(super) fn fatal(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: false,
        }
    }

    pub(super) fn is_retryable(&self) -> bool {
        self.retryable
    }

    pub(super) fn into_message(self) -> String {
        self.message
    }
}

impl Display for TransferError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl From<String> for TransferError {
    fn from(message: String) -> Self {
        Self::fatal(message)
    }
}

impl From<&str> for TransferError {
    fn from(message: &str) -> Self {
        Self::fatal(message)
    }
}

pub(super) trait Transport {
    fn get(&self, url: &Url, offset: Option<u64>) -> Result<Transfer, TransferError>;
}

pub(super) struct ReqwestTransport {
    client: reqwest::Client,
    runtime: Rc<tokio::runtime::Runtime>,
    token: Option<String>,
}

impl ReqwestTransport {
    pub(super) fn new(token: Option<String>) -> Result<Self, String> {
        Self::build(token, READ_IDLE_TIMEOUT)
    }

    #[cfg(test)]
    pub(super) fn with_read_timeout(
        token: Option<String>,
        read_timeout: Duration,
    ) -> Result<Self, String> {
        Self::build(token, read_timeout)
    }

    fn build(token: Option<String>, read_timeout: Duration) -> Result<Self, String> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| error.to_string())?;
        let client = {
            let _guard = runtime.enter();
            reqwest::Client::builder()
                .user_agent(concat!("loxa/", env!("CARGO_PKG_VERSION")))
                .connect_timeout(Duration::from_secs(30))
                .read_timeout(read_timeout)
                .redirect(Policy::none())
                .build()
                .map_err(|error| error.to_string())?
        };
        Ok(Self {
            client,
            runtime: Rc::new(runtime),
            token,
        })
    }
}

impl Transport for ReqwestTransport {
    fn get(&self, url: &Url, offset: Option<u64>) -> Result<Transfer, TransferError> {
        let mut current = url.clone();
        for redirect in 0..=MAX_REDIRECTS {
            let mut request = self.client.get(current.clone());
            if should_attach_token(&current) {
                if let Some(token) = self.token.as_deref() {
                    request = request.bearer_auth(token);
                }
            }
            if let Some(offset) = offset {
                request = request.header(RANGE, format!("bytes={offset}-"));
            }
            let response = self
                .runtime
                .block_on(async { request.send().await })
                .map_err(classify_request_error)?;
            if response.status().is_redirection() {
                if redirect == MAX_REDIRECTS {
                    return Err(TransferError::fatal("too many artifact redirects"));
                }
                let location = response
                    .headers()
                    .get(LOCATION)
                    .ok_or_else(|| TransferError::fatal("artifact redirect omitted Location"))?
                    .to_str()
                    .map_err(|_| TransferError::fatal("invalid artifact redirect"))?;
                current = redirect_target(&current, location)?;
                continue;
            }
            match response.status() {
                StatusCode::UNAUTHORIZED => {
                    return Err(TransferError::fatal(
                        "Hugging Face authentication token is missing or invalid",
                    ));
                }
                StatusCode::FORBIDDEN => {
                    return Err(TransferError::fatal(
                        "Hugging Face access denied; check gated model access",
                    ));
                }
                status if status.is_success() => {
                    let content_range = response
                        .headers()
                        .get(CONTENT_RANGE)
                        .map(|value| value.to_str().map(str::to_string))
                        .transpose()
                        .map_err(|_| TransferError::fatal("invalid Content-Range"))?;
                    return Ok(Transfer {
                        status,
                        content_range,
                        reader: Box::new(ResponseReader::new(Rc::clone(&self.runtime), response)),
                    });
                }
                status if transient_status(status) => {
                    return Err(TransferError::retryable(format!(
                        "artifact server returned HTTP {status}"
                    )));
                }
                status => {
                    return Err(TransferError::fatal(format!(
                        "artifact server returned HTTP {status}"
                    )));
                }
            }
        }
        Err(TransferError::fatal("too many artifact redirects"))
    }
}

struct ResponseReader {
    response: reqwest::Response,
    runtime: Rc<tokio::runtime::Runtime>,
    buffered: Vec<u8>,
    position: usize,
}

impl ResponseReader {
    fn new(runtime: Rc<tokio::runtime::Runtime>, response: reqwest::Response) -> Self {
        Self {
            response,
            runtime,
            buffered: Vec::new(),
            position: 0,
        }
    }
}

impl Read for ResponseReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        while self.position == self.buffered.len() {
            let runtime = Rc::clone(&self.runtime);
            match runtime.block_on(async { self.response.chunk().await }) {
                Ok(Some(chunk)) if !chunk.is_empty() => {
                    self.buffered.clear();
                    self.buffered.extend_from_slice(&chunk);
                    self.position = 0;
                }
                Ok(Some(_)) => continue,
                Ok(None) => return Ok(0),
                Err(error) => {
                    let kind = if error.is_timeout() {
                        io::ErrorKind::TimedOut
                    } else {
                        io::ErrorKind::Other
                    };
                    return Err(io::Error::new(kind, "artifact response body failed"));
                }
            }
        }
        let available = &self.buffered[self.position..];
        let copied = available.len().min(output.len());
        output[..copied].copy_from_slice(&available[..copied]);
        self.position += copied;
        Ok(copied)
    }
}

pub(crate) fn artifact_url(file: &ResolvedFile) -> Result<Url, String> {
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

fn classify_request_error(error: reqwest::Error) -> TransferError {
    if error.is_timeout() || error.is_connect() || (error.is_request() && !error.is_builder()) {
        TransferError::retryable("artifact request failed")
    } else {
        TransferError::fatal("artifact request could not be sent")
    }
}

pub(super) fn redirect_target(current: &Url, location: &str) -> Result<Url, TransferError> {
    let target = current
        .join(location)
        .map_err(|_| TransferError::fatal("invalid artifact redirect"))?;
    if target.scheme() != "https" {
        return Err(TransferError::fatal("artifact redirect must use HTTPS"));
    }
    Ok(target)
}

pub(super) fn transient_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::REQUEST_TIMEOUT
            | StatusCode::TOO_MANY_REQUESTS
            | StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT
    )
}
