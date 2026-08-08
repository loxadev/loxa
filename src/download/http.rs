use crate::huggingface::{should_attach_token, ResolvedFile};
use reqwest::header::{CONTENT_RANGE, LOCATION, RANGE};
use reqwest::{redirect::Policy, StatusCode, Url};
use std::fmt::{self, Display};
use std::future::Future;
use std::time::Duration;

const MAX_REDIRECTS: usize = 5;
const READ_IDLE_TIMEOUT: Duration = Duration::from_secs(120);
const CONTROL_POLL_INTERVAL: Duration = Duration::from_millis(25);

pub(super) enum WaitOutcome<T> {
    Ready(T),
    Paused,
}

pub(super) async fn wait_with_pause<F>(
    future: F,
    should_pause: &impl Fn() -> bool,
) -> WaitOutcome<F::Output>
where
    F: Future,
{
    let mut future = std::pin::pin!(future);
    loop {
        if should_pause() {
            return WaitOutcome::Paused;
        }
        if let Ok(value) = tokio::time::timeout(CONTROL_POLL_INTERVAL, &mut future).await {
            return if should_pause() {
                WaitOutcome::Paused
            } else {
                WaitOutcome::Ready(value)
            };
        }
    }
}

pub(super) struct Transfer {
    pub(super) status: StatusCode,
    pub(super) content_range: Option<String>,
    location: Option<String>,
    response: ResponseBody,
    pending: Vec<u8>,
}

impl Transfer {
    pub(super) async fn chunk(&mut self, limit: usize) -> Result<Option<Vec<u8>>, TransferError> {
        if !self.pending.is_empty() {
            return Ok(Some(self.take_pending(limit)));
        }
        let chunk = match &mut self.response {
            ResponseBody::Reqwest(response) => response
                .chunk()
                .await
                .map(|chunk| chunk.map(|chunk| chunk.to_vec()))
                .map_err(classify_body_error),
            #[cfg(test)]
            ResponseBody::Test(chunks) => match chunks.pop_front() {
                Some(chunk) => chunk.map(Some),
                None => Ok(None),
            },
            #[cfg(test)]
            ResponseBody::TestFutures(futures) => match futures.pop_front() {
                Some(future) => future.await,
                None => Ok(None),
            },
        }?;
        match chunk {
            Some(chunk) if chunk.len() > limit => {
                self.pending.extend_from_slice(&chunk[limit..]);
                Ok(Some(chunk[..limit].to_vec()))
            }
            Some(chunk) => Ok(Some(chunk)),
            None => Ok(None),
        }
    }

    fn take_pending(&mut self, limit: usize) -> Vec<u8> {
        let take = self.pending.len().min(limit);
        self.pending.drain(..take).collect()
    }

    #[cfg(test)]
    pub(super) fn test(
        status: StatusCode,
        content_range: Option<&str>,
        chunks: impl IntoIterator<Item = Result<Vec<u8>, TransferError>>,
    ) -> Self {
        Self {
            status,
            content_range: content_range.map(str::to_string),
            location: None,
            response: ResponseBody::Test(chunks.into_iter().collect()),
            pending: Vec::new(),
        }
    }

    #[cfg(test)]
    pub(super) fn test_redirect(status: StatusCode, location: &str) -> Self {
        Self {
            status,
            content_range: None,
            location: Some(location.to_owned()),
            response: ResponseBody::Test(Default::default()),
            pending: Vec::new(),
        }
    }

    #[cfg(test)]
    pub(super) fn test_future(
        status: StatusCode,
        content_range: Option<&str>,
        future: impl Future<Output = Result<Option<Vec<u8>>, TransferError>> + 'static,
    ) -> Self {
        Self::test_futures(status, content_range, [Box::pin(future) as TestChunkFuture])
    }

    #[cfg(test)]
    pub(super) fn test_futures(
        status: StatusCode,
        content_range: Option<&str>,
        futures: impl IntoIterator<Item = TestChunkFuture>,
    ) -> Self {
        Self {
            status,
            content_range: content_range.map(str::to_string),
            location: None,
            response: ResponseBody::TestFutures(futures.into_iter().collect()),
            pending: Vec::new(),
        }
    }
}

#[cfg(test)]
pub(super) type TestChunkFuture =
    std::pin::Pin<Box<dyn Future<Output = Result<Option<Vec<u8>>, TransferError>> + 'static>>;

enum ResponseBody {
    Reqwest(reqwest::Response),
    #[cfg(test)]
    Test(std::collections::VecDeque<Result<Vec<u8>, TransferError>>),
    #[cfg(test)]
    TestFutures(std::collections::VecDeque<TestChunkFuture>),
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct TransferError {
    message: String,
    retryable: bool,
    paused: bool,
    retained_bytes: Option<u64>,
}

impl TransferError {
    pub(super) fn retryable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: true,
            paused: false,
            retained_bytes: None,
        }
    }

    pub(super) fn fatal(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: false,
            paused: false,
            retained_bytes: None,
        }
    }

    pub(super) fn paused(retained_bytes: u64) -> Self {
        Self {
            message: "artifact transfer paused".into(),
            retryable: false,
            paused: true,
            retained_bytes: Some(retained_bytes),
        }
    }

    pub(super) fn is_retryable(&self) -> bool {
        self.retryable
    }

    pub(super) fn is_paused(&self) -> bool {
        self.paused
    }

    pub(super) fn retained_bytes(&self) -> Option<u64> {
        self.retained_bytes
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

impl std::error::Error for TransferError {}

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
    async fn get(
        &self,
        url: &Url,
        offset: Option<u64>,
        should_pause: &impl Fn() -> bool,
    ) -> Result<Transfer, TransferError>;
}

pub(super) struct ReqwestTransport {
    client: reqwest::Client,
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
        let client = reqwest::Client::builder()
            .user_agent(concat!("loxa/", env!("CARGO_PKG_VERSION")))
            .connect_timeout(Duration::from_secs(30))
            .read_timeout(read_timeout)
            .redirect(Policy::none())
            .build()
            .map_err(|error| error.to_string())?;
        Ok(Self { client, token })
    }

    async fn request_once(&self, url: Url, offset: Option<u64>) -> Result<Transfer, TransferError> {
        let mut request = self.client.get(url.clone());
        if should_attach_token(&url) {
            if let Some(token) = self.token.as_deref() {
                request = request.bearer_auth(token);
            }
        }
        if let Some(offset) = offset {
            request = request.header(RANGE, format!("bytes={offset}-"));
        }
        let response = self
            .client
            .execute(
                request
                    .build()
                    .map_err(|_| TransferError::fatal("artifact request could not be sent"))?,
            )
            .await
            .map_err(classify_request_error)?;
        if response.status().is_redirection() {
            let location = response
                .headers()
                .get(LOCATION)
                .ok_or_else(|| TransferError::fatal("artifact redirect omitted Location"))?
                .to_str()
                .map_err(|_| TransferError::fatal("invalid artifact redirect"))?
                .to_owned();
            return Ok(Transfer {
                status: response.status(),
                content_range: None,
                location: Some(location),
                response: ResponseBody::Reqwest(response),
                pending: Vec::new(),
            });
        }
        match response.status() {
            StatusCode::UNAUTHORIZED => Err(TransferError::fatal(
                "Hugging Face authentication token is missing or invalid",
            )),
            StatusCode::FORBIDDEN => Err(TransferError::fatal(
                "Hugging Face access denied; check gated model access",
            )),
            status if status.is_success() => {
                let content_range = response
                    .headers()
                    .get(CONTENT_RANGE)
                    .map(|value| value.to_str().map(str::to_string))
                    .transpose()
                    .map_err(|_| TransferError::fatal("invalid Content-Range"))?;
                Ok(Transfer {
                    status,
                    content_range,
                    location: None,
                    response: ResponseBody::Reqwest(response),
                    pending: Vec::new(),
                })
            }
            status if transient_status(status) => Err(TransferError::retryable(format!(
                "artifact server returned HTTP {status}"
            ))),
            status => Err(TransferError::fatal(format!(
                "artifact server returned HTTP {status}"
            ))),
        }
    }
}

impl Transport for ReqwestTransport {
    async fn get(
        &self,
        url: &Url,
        offset: Option<u64>,
        should_pause: &impl Fn() -> bool,
    ) -> Result<Transfer, TransferError> {
        follow_redirects(url.clone(), offset, should_pause, |current, offset| {
            self.request_once(current, offset)
        })
        .await
    }
}

pub(super) async fn follow_redirects<F, Fut>(
    mut current: Url,
    offset: Option<u64>,
    should_pause: &impl Fn() -> bool,
    mut execute: F,
) -> Result<Transfer, TransferError>
where
    F: FnMut(Url, Option<u64>) -> Fut,
    Fut: Future<Output = Result<Transfer, TransferError>>,
{
    for redirect in 0..=MAX_REDIRECTS {
        if should_pause() {
            return Err(TransferError::paused(0));
        }
        let transfer = execute(current.clone(), offset).await?;
        if !transfer.status.is_redirection() {
            return Ok(transfer);
        }
        if redirect == MAX_REDIRECTS {
            return Err(TransferError::fatal("too many artifact redirects"));
        }
        let location = transfer
            .location
            .as_deref()
            .ok_or_else(|| TransferError::fatal("artifact redirect omitted Location"))?;
        current = redirect_target(&current, location)?;
    }
    Err(TransferError::fatal("too many artifact redirects"))
}

pub(crate) fn artifact_url(file: &ResolvedFile) -> Result<Url, String> {
    let (owner, repo) = file
        .repo()
        .split_once('/')
        .ok_or_else(|| "repository must be owner/repo".to_string())?;
    let mut url = Url::parse("https://huggingface.co").map_err(|error| error.to_string())?;
    url.path_segments_mut()
        .map_err(|_| "invalid Hugging Face origin")?
        .extend([owner, repo, "resolve", file.commit(), file.path()]);
    Ok(url)
}

fn classify_request_error(error: reqwest::Error) -> TransferError {
    if error.is_timeout() || error.is_connect() || (error.is_request() && !error.is_builder()) {
        TransferError::retryable("artifact request failed")
    } else {
        TransferError::fatal("artifact request could not be sent")
    }
}

fn classify_body_error(error: reqwest::Error) -> TransferError {
    if error.is_timeout() || error.is_body() {
        TransferError::retryable("artifact response body failed")
    } else {
        TransferError::fatal("artifact response body failed")
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
