mod tests {
    use super::*;
    use crate::huggingface::{test_resolved_file, ResolvedFile};
    use reqwest::{StatusCode, Url};
    use sha2::{Digest, Sha256};
    use std::cell::{Cell, RefCell};
    use std::io::{Read, Write};
    use tempfile::tempdir;

    struct FakeTransport {
        responses: RefCell<Vec<Transfer>>,
        offsets: RefCell<Vec<Option<u64>>>,
    }

    struct FailingTransport {
        remaining_failures: Cell<usize>,
        retryable: bool,
        attempts: Cell<usize>,
        offsets: RefCell<Vec<Option<u64>>>,
        bytes: Vec<u8>,
    }

    struct RequestGate {
        pause: std::sync::atomic::AtomicBool,
        release: std::sync::atomic::AtomicBool,
        requests: std::sync::atomic::AtomicUsize,
        polls: std::sync::atomic::AtomicUsize,
        offsets: std::sync::Mutex<Vec<Option<u64>>>,
        waker: std::sync::Mutex<Option<std::task::Waker>>,
    }

    struct PausingRequestTransport {
        gate: std::sync::Arc<RequestGate>,
        started: std::sync::Mutex<Option<std::sync::mpsc::Sender<()>>>,
    }

    struct RedirectingTransport<'a> {
        pause: &'a Cell<bool>,
        requests: RefCell<Vec<(Url, Option<u64>, bool)>>,
    }

    struct BodyGate {
        pause: std::sync::atomic::AtomicBool,
        release: std::sync::atomic::AtomicBool,
        polls: std::sync::atomic::AtomicUsize,
        waker: std::sync::Mutex<Option<std::task::Waker>>,
    }

    struct RetryDelayGate {
        pause: std::sync::atomic::AtomicBool,
        release: std::sync::atomic::AtomicBool,
        polls: std::sync::atomic::AtomicUsize,
        delays: std::sync::Mutex<Vec<std::time::Duration>>,
        started: std::sync::Mutex<Option<std::sync::mpsc::Sender<()>>>,
        waker: std::sync::Mutex<Option<std::task::Waker>>,
    }

    struct BodyThenPausingRequestTransport<'a> {
        pause: &'a Cell<bool>,
        attempts: Cell<usize>,
        offsets: RefCell<Vec<Option<u64>>>,
        first: RefCell<Option<Transfer>>,
    }

    struct BodyThenFailingRequestTransport {
        attempts: Cell<usize>,
        offsets: RefCell<Vec<Option<u64>>>,
        first: RefCell<Option<Transfer>>,
        retryable: bool,
    }

    enum SecondRequestMutation {
        Part {
            part: std::path::PathBuf,
            original: std::path::PathBuf,
            replacement: Vec<u8>,
        },
        InPlacePart {
            part: std::path::PathBuf,
            replacement: Vec<u8>,
        },
        Directory {
            model_dir: std::path::PathBuf,
            moved_dir: std::path::PathBuf,
        },
    }

    struct BodyThenMutatingProtocolTransport {
        offsets: RefCell<Vec<Option<u64>>>,
        first: RefCell<Option<Transfer>>,
        mutation: RefCell<Option<SecondRequestMutation>>,
    }

    struct SubstitutingPartTransport {
        part: std::path::PathBuf,
        original: std::path::PathBuf,
        replacement: Option<Vec<u8>>,
        response: RefCell<Option<Transfer>>,
        offsets: RefCell<Vec<Option<u64>>>,
        substituted: Cell<bool>,
    }

    struct SubstitutingDirectoryTransport {
        model_dir: std::path::PathBuf,
        moved_dir: std::path::PathBuf,
        response: RefCell<Option<Transfer>>,
        offsets: RefCell<Vec<Option<u64>>>,
        substituted: Cell<bool>,
    }

    struct CleanupObservingTransport<'a> {
        cleanup_complete: &'a Cell<bool>,
        observed_cleanup: Cell<bool>,
        response: RefCell<Option<Transfer>>,
        offsets: RefCell<Vec<Option<u64>>>,
    }

    #[cfg(unix)]
    struct FifoSubstitutingPartTransport {
        part: std::path::PathBuf,
        original: std::path::PathBuf,
        response: RefCell<Option<Transfer>>,
        offsets: RefCell<Vec<Option<u64>>>,
        substituted: Cell<bool>,
        started: std::sync::Mutex<Option<std::sync::mpsc::Sender<()>>>,
    }

    impl Transport for FakeTransport {
        async fn get(
            &self,
            _url: &Url,
            offset: Option<u64>,
            _should_pause: &impl Fn() -> bool,
        ) -> Result<Transfer, TransferError> {
            self.offsets.borrow_mut().push(offset);
            let mut responses = self.responses.borrow_mut();
            if responses.is_empty() {
                Err(TransferError::fatal("no fake response"))
            } else {
                Ok(responses.remove(0))
            }
        }
    }

    impl Transport for BodyThenPausingRequestTransport<'_> {
        async fn get(
            &self,
            _url: &Url,
            offset: Option<u64>,
            _should_pause: &impl Fn() -> bool,
        ) -> Result<Transfer, TransferError> {
            self.offsets.borrow_mut().push(offset);
            let attempt = self.attempts.get() + 1;
            self.attempts.set(attempt);
            if attempt == 1 {
                return self
                    .first
                    .borrow_mut()
                    .take()
                    .ok_or_else(|| TransferError::fatal("missing first response"));
            }
            self.pause.set(true);
            std::future::pending::<Result<Transfer, TransferError>>().await
        }
    }

    impl Transport for BodyThenFailingRequestTransport {
        async fn get(
            &self,
            _url: &Url,
            offset: Option<u64>,
            _should_pause: &impl Fn() -> bool,
        ) -> Result<Transfer, TransferError> {
            self.offsets.borrow_mut().push(offset);
            let attempt = self.attempts.get() + 1;
            self.attempts.set(attempt);
            if attempt == 1 {
                return self
                    .first
                    .borrow_mut()
                    .take()
                    .ok_or_else(|| TransferError::fatal("missing first response"));
            }
            if self.retryable {
                Err(TransferError::retryable("later request failed"))
            } else {
                Err(TransferError::fatal("later request failed"))
            }
        }
    }

    impl Transport for BodyThenMutatingProtocolTransport {
        async fn get(
            &self,
            _url: &Url,
            offset: Option<u64>,
            _should_pause: &impl Fn() -> bool,
        ) -> Result<Transfer, TransferError> {
            self.offsets.borrow_mut().push(offset);
            if let Some(first) = self.first.borrow_mut().take() {
                return Ok(first);
            }
            match self
                .mutation
                .borrow_mut()
                .take()
                .ok_or_else(|| TransferError::fatal("missing second-request mutation"))?
            {
                SecondRequestMutation::Part {
                    part,
                    original,
                    replacement,
                } => {
                    std::fs::rename(&part, &original)
                        .map_err(|_| TransferError::fatal("part substitution failed"))?;
                    std::fs::write(part, replacement)
                        .map_err(|_| TransferError::fatal("part substitution failed"))?;
                }
                SecondRequestMutation::InPlacePart { part, replacement } => {
                    std::fs::write(part, replacement)
                        .map_err(|_| TransferError::fatal("part substitution failed"))?;
                }
                SecondRequestMutation::Directory {
                    model_dir,
                    moved_dir,
                } => {
                    std::fs::rename(&model_dir, &moved_dir)
                        .map_err(|_| TransferError::fatal("directory substitution failed"))?;
                    std::fs::create_dir(&model_dir)
                        .map_err(|_| TransferError::fatal("directory substitution failed"))?;
                    std::fs::write(model_dir.join("foreign.bin"), b"replacement foreign")
                        .map_err(|_| TransferError::fatal("directory substitution failed"))?;
                }
            }
            Ok(transfer(StatusCode::BAD_REQUEST, None, b""))
        }
    }

    impl Transport for SubstitutingPartTransport {
        async fn get(
            &self,
            _url: &Url,
            offset: Option<u64>,
            _should_pause: &impl Fn() -> bool,
        ) -> Result<Transfer, TransferError> {
            self.offsets.borrow_mut().push(offset);
            std::fs::rename(&self.part, &self.original)
                .map_err(|_| TransferError::fatal("part substitution failed"))?;
            if let Some(replacement) = &self.replacement {
                std::fs::write(&self.part, replacement)
                    .map_err(|_| TransferError::fatal("part substitution failed"))?;
            }
            self.substituted.set(true);
            self.response
                .borrow_mut()
                .take()
                .ok_or_else(|| TransferError::fatal("missing substitution response"))
        }
    }

    impl Transport for SubstitutingDirectoryTransport {
        async fn get(
            &self,
            _url: &Url,
            offset: Option<u64>,
            _should_pause: &impl Fn() -> bool,
        ) -> Result<Transfer, TransferError> {
            self.offsets.borrow_mut().push(offset);
            std::fs::rename(&self.model_dir, &self.moved_dir)
                .map_err(|_| TransferError::fatal("directory substitution failed"))?;
            std::fs::create_dir(&self.model_dir)
                .map_err(|_| TransferError::fatal("directory substitution failed"))?;
            std::fs::write(self.model_dir.join("foreign.bin"), b"replacement foreign")
                .map_err(|_| TransferError::fatal("directory substitution failed"))?;
            self.substituted.set(true);
            self.response
                .borrow_mut()
                .take()
                .ok_or_else(|| TransferError::fatal("missing directory response"))
        }
    }

    impl Transport for CleanupObservingTransport<'_> {
        async fn get(
            &self,
            _url: &Url,
            offset: Option<u64>,
            _should_pause: &impl Fn() -> bool,
        ) -> Result<Transfer, TransferError> {
            self.offsets.borrow_mut().push(offset);
            self.observed_cleanup.set(self.cleanup_complete.get());
            self.response
                .borrow_mut()
                .take()
                .ok_or_else(|| TransferError::fatal("missing cleanup-observer response"))
        }
    }

    #[cfg(unix)]
    impl Transport for FifoSubstitutingPartTransport {
        async fn get(
            &self,
            _url: &Url,
            offset: Option<u64>,
            _should_pause: &impl Fn() -> bool,
        ) -> Result<Transfer, TransferError> {
            use std::os::unix::ffi::OsStrExt;

            self.offsets.borrow_mut().push(offset);
            std::fs::rename(&self.part, &self.original)
                .map_err(|_| TransferError::fatal("part substitution failed"))?;
            let path = std::ffi::CString::new(self.part.as_os_str().as_bytes())
                .map_err(|_| TransferError::fatal("part substitution failed"))?;
            if unsafe { libc::mkfifo(path.as_ptr(), 0o600) } != 0 {
                return Err(TransferError::fatal("part substitution failed"));
            }
            self.substituted.set(true);
            if let Some(started) = self.started.lock().unwrap().take() {
                let _ = started.send(());
            }
            self.response
                .borrow_mut()
                .take()
                .ok_or_else(|| TransferError::fatal("missing substitution response"))
        }
    }

    impl Transport for FailingTransport {
        async fn get(
            &self,
            _url: &Url,
            offset: Option<u64>,
            _should_pause: &impl Fn() -> bool,
        ) -> Result<Transfer, TransferError> {
            self.attempts.set(self.attempts.get() + 1);
            self.offsets.borrow_mut().push(offset);
            if self.remaining_failures.get() > 0 {
                self.remaining_failures
                    .set(self.remaining_failures.get() - 1);
                if self.retryable {
                    Err(TransferError::retryable("transient request failure"))
                } else {
                    Err(TransferError::fatal("fatal request failure"))
                }
            } else {
                Ok(transfer(StatusCode::OK, None, &self.bytes))
            }
        }
    }

    impl Transport for PausingRequestTransport {
        async fn get(
            &self,
            _url: &Url,
            offset: Option<u64>,
            _should_pause: &impl Fn() -> bool,
        ) -> Result<Transfer, TransferError> {
            use std::sync::atomic::Ordering;

            self.gate.requests.fetch_add(1, Ordering::SeqCst);
            self.gate.offsets.lock().unwrap().push(offset);
            if let Some(started) = self.started.lock().unwrap().take() {
                let _ = started.send(());
            }
            std::future::poll_fn(|context| {
                self.gate.polls.fetch_add(1, Ordering::SeqCst);
                self.gate.pause.store(true, Ordering::SeqCst);
                if self.gate.release.load(Ordering::SeqCst) {
                    std::task::Poll::Ready(Err(TransferError::fatal(
                        "released request future",
                    )))
                } else {
                    *self.gate.waker.lock().unwrap() = Some(context.waker().clone());
                    std::task::Poll::Pending
                }
            })
            .await
        }
    }

    impl Transport for RedirectingTransport<'_> {
        async fn get(
            &self,
            url: &Url,
            offset: Option<u64>,
            should_pause: &impl Fn() -> bool,
        ) -> Result<Transfer, TransferError> {
            follow_redirects(url.clone(), offset, should_pause, |current, offset| {
                let mut requests = self.requests.borrow_mut();
                requests.push((
                    current.clone(),
                    offset,
                    crate::huggingface::should_attach_token(&current),
                ));
                let response = if requests.len() == 1 {
                    self.pause.set(true);
                    Ok(Transfer::test_redirect(
                        StatusCode::FOUND,
                        "https://cdn.example/artifact.gguf",
                    ))
                } else {
                    Err(TransferError::fatal("redirect issued a later request"))
                };
                std::future::ready(response)
            })
            .await
        }
    }

    fn transfer(status: StatusCode, range: Option<&str>, bytes: &[u8]) -> Transfer {
        Transfer::test(status, range, [Ok(bytes.to_vec())])
    }

    fn failing_transfer(status: StatusCode, range: Option<&str>, prefix: &[u8]) -> Transfer {
        Transfer::test(
            status,
            range,
            [
                Ok(prefix.to_vec()),
                Err(TransferError::retryable("artifact response body failed")),
            ],
        )
    }

    fn body_future(
        future: impl std::future::Future<Output = Result<Option<Vec<u8>>, TransferError>> + 'static,
    ) -> http::TestChunkFuture {
        Box::pin(future)
    }

    fn spec(bytes: &[u8]) -> ResolvedFile {
        test_resolved_file(hex(Sha256::digest(bytes).as_ref()), bytes.len() as u64)
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct ArtifactSnapshotEntry {
        name: String,
        bytes: Vec<u8>,
        #[cfg(unix)]
        device: u64,
        #[cfg(unix)]
        inode: u64,
        #[cfg(unix)]
        links: u64,
    }

    fn artifact_snapshot(path: &std::path::Path) -> Vec<ArtifactSnapshotEntry> {
        let mut entries = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                let metadata = entry.metadata().unwrap();
                #[cfg(unix)]
                use std::os::unix::fs::MetadataExt;
                ArtifactSnapshotEntry {
                    name: entry.file_name().into_string().unwrap(),
                    bytes: std::fs::read(entry.path()).unwrap(),
                    #[cfg(unix)]
                    device: metadata.dev(),
                    #[cfg(unix)]
                    inode: metadata.ino(),
                    #[cfg(unix)]
                    links: metadata.nlink(),
                }
            })
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.name.cmp(&right.name));
        entries
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
    fn progress_reports_existing_and_new_bytes_for_a_resumed_transfer() {
        let bytes = b"abcdef";
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("model.gguf.part"), b"abc").unwrap();
        let transport = FakeTransport {
            responses: RefCell::new(vec![transfer(
                StatusCode::PARTIAL_CONTENT,
                Some("bytes 3-5/6"),
                b"def",
            )]),
            offsets: RefCell::new(Vec::new()),
        };
        let mut updates = Vec::new();

        download_with_transport_progress(&spec(bytes), dir.path(), &transport, |update| {
            updates.push(update);
        })
        .unwrap();

        assert_eq!(
            updates,
            [
                ProgressUpdate::Transferring {
                    transferred: 3,
                    total: 6,
                },
                ProgressUpdate::Transferring {
                    transferred: 6,
                    total: 6,
                },
                ProgressUpdate::Verifying {
                    transferred: 6,
                    total: 6,
                },
            ]
        );
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
    fn valid_final_completes_interrupted_repair_cleanup() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("model.gguf"), b"abcdef").unwrap();
        std::fs::write(dir.path().join("model.gguf.invalid"), b"corrupt").unwrap();
        std::fs::write(dir.path().join("model.gguf.part.restart"), b"stale").unwrap();
        let transport = FakeTransport {
            responses: RefCell::new(Vec::new()),
            offsets: RefCell::new(Vec::new()),
        };

        let mut updates = Vec::new();
        let final_path = download_with_transport_progress(
            &spec(b"abcdef"),
            dir.path(),
            &transport,
            |update| updates.push(update),
        )
        .unwrap();

        assert!(matches!(
            &final_path,
            DownloadOutcome::AlreadyInstalled(_)
        ));
        assert_eq!(std::fs::read(final_path.as_ref()).unwrap(), b"abcdef");
        assert_eq!(
            updates,
            [ProgressUpdate::Verifying {
                transferred: 6,
                total: 6,
            }]
        );
        assert!(!dir.path().join("model.gguf.invalid").exists());
        assert!(!dir.path().join("model.gguf.part.restart").exists());
        assert!(transport.offsets.borrow().is_empty());
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
    fn interrupted_body_retries_from_the_durable_prefix_in_one_pull() {
        let bytes = b"abcdef";
        let dir = tempdir().unwrap();
        let transport = FakeTransport {
            responses: RefCell::new(vec![
                failing_transfer(StatusCode::OK, None, b"abc"),
                transfer(StatusCode::PARTIAL_CONTENT, Some("bytes 3-5/6"), b"def"),
            ]),
            offsets: RefCell::new(Vec::new()),
        };

        let final_path = download_with_transport(&spec(bytes), dir.path(), &transport).unwrap();

        assert_eq!(std::fs::read(final_path).unwrap(), bytes);
        assert_eq!(&*transport.offsets.borrow(), &[None, Some(3)]);
    }

    #[test]
    fn retryable_request_failures_are_bounded_and_fatal_failures_stop_immediately() {
        let bytes = b"abcdef";
        let retry_dir = tempdir().unwrap();
        let retrying = FailingTransport {
            remaining_failures: Cell::new(3),
            retryable: true,
            attempts: Cell::new(0),
            offsets: RefCell::new(Vec::new()),
            bytes: bytes.to_vec(),
        };

        download_with_transport(&spec(bytes), retry_dir.path(), &retrying).unwrap();

        assert_eq!(retrying.attempts.get(), 4);
        assert_eq!(&*retrying.offsets.borrow(), &[None, None, None, None]);

        let exhausted_dir = tempdir().unwrap();
        let exhausted = FailingTransport {
            remaining_failures: Cell::new(10),
            retryable: true,
            attempts: Cell::new(0),
            offsets: RefCell::new(Vec::new()),
            bytes: bytes.to_vec(),
        };

        let error =
            download_with_transport(&spec(bytes), exhausted_dir.path(), &exhausted).unwrap_err();

        assert_eq!(error, "transient request failure");
        assert_eq!(exhausted.attempts.get(), 4);

        let fatal_dir = tempdir().unwrap();
        let fatal = FailingTransport {
            remaining_failures: Cell::new(1),
            retryable: false,
            attempts: Cell::new(0),
            offsets: RefCell::new(Vec::new()),
            bytes: bytes.to_vec(),
        };

        let error = download_with_transport(&spec(bytes), fatal_dir.path(), &fatal).unwrap_err();

        assert_eq!(error, "fatal request failure");
        assert_eq!(fatal.attempts.get(), 1);
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

        assert_eq!(error, "artifact response body failed");
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
        let transport = FakeTransport {
            responses: RefCell::new(vec![Transfer::test(
                StatusCode::OK,
                None,
                [Ok(vec![b'x'; 64])],
            )]),
            offsets: RefCell::new(Vec::new()),
        };

        assert!(download_with_transport(&spec(b"abcdef"), dir.path(), &transport).is_err());

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
        });
        let transport = ReqwestTransport::new(None).unwrap();
        let url = Url::parse(&format!("http://{address}/start")).unwrap();

        let error = match test_runtime().block_on(transport.get(&url, None, &|| false)) {
            Ok(_) => panic!("the redirected request must fail"),
            Err(error) => error,
        };
        server.join().unwrap();

        let error = error.to_string();
        assert!(!error.contains('?'), "{error}");
        assert!(!error.contains("X-Amz-Credential"), "{error}");
        assert!(!error.contains("secret-token"), "{error}");
    }

    #[test]
    fn connected_stalled_body_hits_the_read_idle_timeout() {
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut connection, _) = listener.accept().unwrap();
            let mut request = [0_u8; 1];
            connection.read_exact(&mut request).unwrap();
            connection
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\nabc",
                )
                .unwrap();
            connection.flush().unwrap();
            std::thread::sleep(std::time::Duration::from_millis(400));
        });
        let transport = ReqwestTransport::with_read_timeout(
            None,
            std::time::Duration::from_millis(100),
        )
        .unwrap();
        let url = Url::parse(&format!("http://{address}/artifact")).unwrap();
        let (prefix, error, elapsed) = test_runtime().block_on(async {
            let mut transfer = transport.get(&url, None, &|| false).await.unwrap();
            let prefix = transfer.chunk(3).await.unwrap().unwrap();
            let started = std::time::Instant::now();
            let error = transfer.chunk(3).await.unwrap_err();
            (prefix, error, started.elapsed())
        });

        server.join().unwrap();
        assert_eq!(prefix, b"abc");
        assert_eq!(error.to_string(), "artifact response body failed");
        assert!(elapsed < std::time::Duration::from_millis(350));
    }

    #[test]
    fn artifact_redirects_cannot_downgrade_https() {
        let origin =
            Url::parse("https://huggingface.co/owner/repo/resolve/rev/model.gguf").unwrap();

        let error = redirect_target(&origin, "http://cdn.example/model.gguf").unwrap_err();

        assert!(error.to_string().contains("HTTPS"));
        assert_eq!(
            redirect_target(&origin, "https://cdn.example/model.gguf").unwrap(),
            Url::parse("https://cdn.example/model.gguf").unwrap()
        );
    }

    #[test]
    fn only_transient_http_statuses_are_retried() {
        for status in [408, 429, 500, 502, 503, 504] {
            assert!(transient_status(StatusCode::from_u16(status).unwrap()));
        }
        for status in [400, 401, 403, 404] {
            assert!(!transient_status(StatusCode::from_u16(status).unwrap()));
        }
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

    #[test]
    fn ignored_range_pause_promotes_the_longer_durable_restart_without_losing_the_old_part() {
        let expected = b"abcdefghi";
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("model.gguf.part"), b"abc").unwrap();
        let transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, b"abcdef")]),
            offsets: RefCell::new(Vec::new()),
        };
        let pause = Cell::new(false);
        let retry_notifications = Cell::new(0);
        let checkpoints = RefCell::new(Vec::new());
        let sync_checkpoints = RefCell::new(Vec::new());

        let outcome = download_with_transport_controlled(
            &spec(expected),
            dir.path(),
            &transport,
            || pause.get(),
            |update| {
                if matches!(
                    update,
                    ProgressUpdate::Transferring {
                        transferred: 6,
                        ..
                    }
                ) {
                    pause.set(true);
                }
            },
            RetryWait {
                observer: |_| retry_notifications.set(retry_notifications.get() + 1),
                sleep: tokio::time::sleep,
            },
            |operation| {
                match &operation {
                    ArtifactOperation::Sync { checkpoint, .. } => {
                        checkpoints.borrow_mut().push(*checkpoint);
                        sync_checkpoints.borrow_mut().push(*checkpoint);
                    }
                    ArtifactOperation::Observe(checkpoint) => {
                        checkpoints.borrow_mut().push(*checkpoint);
                    }
                    ArtifactOperation::Write { .. } => {}
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap();

        assert_eq!(
            outcome,
            DownloadTerminalOutcome::Paused { retained_bytes: 6 }
        );
        assert_eq!(&*transport.offsets.borrow(), &[Some(3)]);
        assert_eq!(retry_notifications.get(), 0);
        assert_eq!(
            std::fs::read(dir.path().join("model.gguf.part")).unwrap(),
            b"abcdef"
        );
        assert!(!dir.path().join("model.gguf.part.restart").exists());
        assert!(!dir.path().join("model.gguf").exists());
        assert_eq!(
            &*checkpoints.borrow(),
            &[
                ArtifactCheckpoint::StagingSynced,
                ArtifactCheckpoint::StagingIdentityMatched,
                ArtifactCheckpoint::BeforePrefixExchange,
                ArtifactCheckpoint::PrefixExchanged,
                ArtifactCheckpoint::DirectorySynced,
                ArtifactCheckpoint::AuthoritativeRestatted,
                ArtifactCheckpoint::BeforeRestartUnlink,
                ArtifactCheckpoint::RestartIdentityMatched,
                ArtifactCheckpoint::RestartUnlinked,
                ArtifactCheckpoint::NormalizationDirectorySynced,
                ArtifactCheckpoint::BeforeRestartAbsenceProof,
                ArtifactCheckpoint::RestartAbsent,
            ]
        );
        assert_eq!(
            &*sync_checkpoints.borrow(),
            &[
                ArtifactCheckpoint::StagingSynced,
                ArtifactCheckpoint::DirectorySynced,
                ArtifactCheckpoint::NormalizationDirectorySynced,
            ]
        );

        let hash_size = 3 * 64 * 1024 + 17;
        let hash_bytes = (0..hash_size)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let hash_dir = tempdir().unwrap();
        std::fs::write(
            hash_dir.path().join("model.gguf.part"),
            &hash_bytes[..3],
        )
        .unwrap();
        let hash_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, &hash_bytes)]),
            offsets: RefCell::new(Vec::new()),
        };
        let hash_verifying = Cell::new(false);
        let hash_checks = Cell::new(0);
        let hash_retries = Cell::new(0);
        let hash_pause = download_with_transport_controlled(
            &spec(&hash_bytes),
            hash_dir.path(),
            &hash_transport,
            || {
                if !hash_verifying.get() {
                    return false;
                }
                let next = hash_checks.get() + 1;
                hash_checks.set(next);
                next >= 3
            },
            |update| {
                if matches!(update, ProgressUpdate::Verifying { .. }) {
                    hash_verifying.set(true);
                }
            },
            RetryWait {
                observer: |_| hash_retries.set(hash_retries.get() + 1),
                sleep: tokio::time::sleep,
            },
            perform_artifact_operation,
        )
        .unwrap();
        assert_eq!(
            hash_pause,
            DownloadTerminalOutcome::Paused {
                retained_bytes: hash_size as u64,
            }
        );
        assert!(hash_checks.get() >= 3);
        assert_eq!(&*hash_transport.offsets.borrow(), &[Some(3)]);
        assert_eq!(hash_retries.get(), 0);
        assert_eq!(
            std::fs::read(hash_dir.path().join("model.gguf.part")).unwrap(),
            hash_bytes
        );
        assert!(!hash_dir.path().join("model.gguf.part.restart").exists());
        assert!(!hash_dir.path().join("model.gguf").exists());

        let fence_dir = tempdir().unwrap();
        std::fs::write(fence_dir.path().join("model.gguf.part"), b"abc").unwrap();
        let fence_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, expected)]),
            offsets: RefCell::new(Vec::new()),
        };
        let fence_pause_requested = Cell::new(false);
        let fence_retries = Cell::new(0);
        let fence_pause = download_with_transport_controlled(
            &spec(expected),
            fence_dir.path(),
            &fence_transport,
            || fence_pause_requested.get(),
            |_| {},
            RetryWait {
                observer: |_| fence_retries.set(fence_retries.get() + 1),
                sleep: tokio::time::sleep,
            },
            |operation| {
                if matches!(
                    &operation,
                    ArtifactOperation::Observe(ArtifactCheckpoint::BeforePromotion)
                ) {
                    fence_pause_requested.set(true);
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap();
        assert_eq!(
            fence_pause,
            DownloadTerminalOutcome::Paused { retained_bytes: 9 }
        );
        assert_eq!(&*fence_transport.offsets.borrow(), &[Some(3)]);
        assert_eq!(fence_retries.get(), 0);
        assert_eq!(
            std::fs::read(fence_dir.path().join("model.gguf.part")).unwrap(),
            expected
        );
        assert!(!fence_dir.path().join("model.gguf.part.restart").exists());
        assert!(!fence_dir.path().join("model.gguf").exists());

        #[derive(Clone, Copy)]
        enum PreservedPhase {
            BeforeExchange,
            AfterExchange,
            AfterOldPartUnlink,
        }

        let failure_points = [
            (ArtifactCheckpoint::StagingSynced, PreservedPhase::BeforeExchange),
            (
                ArtifactCheckpoint::StagingIdentityMatched,
                PreservedPhase::BeforeExchange,
            ),
            (
                ArtifactCheckpoint::BeforePrefixExchange,
                PreservedPhase::BeforeExchange,
            ),
            (ArtifactCheckpoint::PrefixExchanged, PreservedPhase::AfterExchange),
            (ArtifactCheckpoint::DirectorySynced, PreservedPhase::AfterExchange),
            (
                ArtifactCheckpoint::AuthoritativeRestatted,
                PreservedPhase::AfterExchange,
            ),
            (
                ArtifactCheckpoint::BeforeRestartUnlink,
                PreservedPhase::AfterExchange,
            ),
            (
                ArtifactCheckpoint::RestartIdentityMatched,
                PreservedPhase::AfterExchange,
            ),
            (
                ArtifactCheckpoint::RestartUnlinked,
                PreservedPhase::AfterOldPartUnlink,
            ),
            (
                ArtifactCheckpoint::NormalizationDirectorySynced,
                PreservedPhase::AfterOldPartUnlink,
            ),
            (
                ArtifactCheckpoint::BeforeRestartAbsenceProof,
                PreservedPhase::AfterOldPartUnlink,
            ),
            (
                ArtifactCheckpoint::RestartAbsent,
                PreservedPhase::AfterOldPartUnlink,
            ),
        ];

        for (failure_point, phase) in failure_points {
            let failure_dir = tempdir().unwrap();
            let part = failure_dir.path().join("model.gguf.part");
            std::fs::write(&part, b"abc").unwrap();
            for (name, bytes) in [
                ("model.gguf.invalid", b"invalid evidence".as_slice()),
                ("model.lock", b"catalog authority".as_slice()),
                ("foreign.bin", b"foreign".as_slice()),
                ("outside", b"outside".as_slice()),
            ] {
                std::fs::write(failure_dir.path().join(name), bytes).unwrap();
            }
            let initial = artifact_snapshot(failure_dir.path());
            let initial_part = initial
                .iter()
                .find(|entry| entry.name == "model.gguf.part")
                .unwrap();
            let initial_witnesses = initial
                .iter()
                .filter(|entry| entry.name != "model.gguf.part")
                .cloned()
                .collect::<Vec<_>>();
            let failure_transport = FakeTransport {
                responses: RefCell::new(vec![transfer(StatusCode::OK, None, b"abcdef")]),
                offsets: RefCell::new(Vec::new()),
            };
            let failure_pause = Cell::new(false);
            let failure_retries = Cell::new(0);
            let before_exchange = RefCell::new(None);
            let after_exchange = RefCell::new(None);
            let after_unlink = RefCell::new(None);

            let failure = download_with_transport_controlled(
                &spec(expected),
                failure_dir.path(),
                &failure_transport,
                || failure_pause.get(),
                |update| {
                    if matches!(
                        update,
                        ProgressUpdate::Transferring {
                            transferred: 6,
                            ..
                        }
                    ) {
                        failure_pause.set(true);
                    }
                },
                RetryWait {
                    observer: |_| failure_retries.set(failure_retries.get() + 1),
                    sleep: tokio::time::sleep,
                },
                |operation| {
                    let checkpoint = match &operation {
                        ArtifactOperation::Sync { checkpoint, .. }
                        | ArtifactOperation::Observe(checkpoint) => Some(*checkpoint),
                        ArtifactOperation::Write { .. } => None,
                    };
                    if checkpoint == Some(ArtifactCheckpoint::StagingSynced)
                        && before_exchange.borrow().is_none()
                    {
                        *before_exchange.borrow_mut() =
                            Some(artifact_snapshot(failure_dir.path()));
                    }
                    if checkpoint == Some(ArtifactCheckpoint::PrefixExchanged)
                        && after_exchange.borrow().is_none()
                    {
                        *after_exchange.borrow_mut() =
                            Some(artifact_snapshot(failure_dir.path()));
                    }
                    if checkpoint == Some(ArtifactCheckpoint::RestartUnlinked)
                        && after_unlink.borrow().is_none()
                    {
                        *after_unlink.borrow_mut() = Some(artifact_snapshot(failure_dir.path()));
                    }
                    if checkpoint == Some(failure_point) {
                        return Err(ArtifactOperationFailure::Other);
                    }
                    perform_artifact_operation(operation)
                },
            )
            .unwrap_err();

            assert_eq!(failure, DownloadFailure::Durability);
            assert_eq!(&*failure_transport.offsets.borrow(), &[Some(3)]);
            assert_eq!(failure_retries.get(), 0);
            assert!(!failure_dir.path().join("model.gguf").exists());
            let after = artifact_snapshot(failure_dir.path());
            let expected_snapshot = match phase {
                PreservedPhase::BeforeExchange => before_exchange.borrow().clone().unwrap(),
                PreservedPhase::AfterExchange => after_exchange.borrow().clone().unwrap(),
                PreservedPhase::AfterOldPartUnlink => after_unlink.borrow().clone().unwrap(),
            };
            assert_eq!(after, expected_snapshot);
            let after_witnesses = after
                .iter()
                .filter(|entry| {
                    entry.name != "model.gguf.part"
                        && entry.name != "model.gguf.part.restart"
                })
                .cloned()
                .collect::<Vec<_>>();
            assert_eq!(after_witnesses, initial_witnesses);

            let after_part = after
                .iter()
                .find(|entry| entry.name == "model.gguf.part")
                .unwrap();
            let after_restart = after
                .iter()
                .find(|entry| entry.name == "model.gguf.part.restart");
            match phase {
                PreservedPhase::BeforeExchange => {
                    assert_eq!(after_part.bytes, b"abc");
                    assert_eq!(after_restart.unwrap().bytes, b"abcdef");
                    #[cfg(unix)]
                    assert_eq!(after_part.inode, initial_part.inode);
                }
                PreservedPhase::AfterExchange => {
                    assert_eq!(after_part.bytes, b"abcdef");
                    let after_restart = after_restart.unwrap();
                    assert_eq!(after_restart.bytes, b"abc");
                    #[cfg(unix)]
                    assert_eq!(after_restart.inode, initial_part.inode);
                }
                PreservedPhase::AfterOldPartUnlink => {
                    assert_eq!(after_part.bytes, b"abcdef");
                    assert!(after_restart.is_none());
                }
            }

            let (directory, _) =
                crate::safe_file::open_directory(failure_dir.path()).unwrap();
            let sha256 = hex(Sha256::digest(expected).as_ref());
            let plan = match plan::plan_artifact_transfer(
                &directory,
                failure_dir.path(),
                expected.len() as u64,
                &sha256,
                &|| false,
            ) {
                plan::ArtifactPlanOutcome::Ready(plan) => plan,
                plan::ArtifactPlanOutcome::Interrupted => {
                    panic!("inert fresh artifact audit interrupted")
                }
            };
            assert_eq!(plan.state(), plan::ArtifactTransferState::InstalledRepair);
            assert_eq!(
                plan.part_length(),
                Some(match phase {
                    PreservedPhase::BeforeExchange => 3,
                    PreservedPhase::AfterExchange | PreservedPhase::AfterOldPartUnlink => 6,
                })
            );
            assert_eq!(
                plan.restart_length(),
                match phase {
                    PreservedPhase::BeforeExchange => Some(6),
                    PreservedPhase::AfterExchange => Some(3),
                    PreservedPhase::AfterOldPartUnlink => None,
                }
            );
            assert!(plan.has_invalid_authority());
        }

        for substitution_point in [
            ArtifactCheckpoint::BeforeRestartUnlink,
            ArtifactCheckpoint::RestartIdentityMatched,
        ] {
            let substitution_dir = tempdir().unwrap();
            let normalized_part = substitution_dir.path().join("model.gguf.part");
            let normalized_original = substitution_dir.path().join("normalized-original");
            std::fs::write(&normalized_part, b"abc").unwrap();
            let substitution_transport = FakeTransport {
                responses: RefCell::new(vec![transfer(StatusCode::OK, None, b"abcdef")]),
                offsets: RefCell::new(Vec::new()),
            };
            let substitution_pause = Cell::new(false);
            let substitution_retries = Cell::new(0);
            let substituted = Cell::new(false);

            let failure = download_with_transport_controlled(
                &spec(expected),
                substitution_dir.path(),
                &substitution_transport,
                || substitution_pause.get(),
                |update| {
                    if matches!(
                        update,
                        ProgressUpdate::Transferring {
                            transferred: 6,
                            ..
                        }
                    ) {
                        substitution_pause.set(true);
                    }
                },
                RetryWait {
                    observer: |_| substitution_retries.set(substitution_retries.get() + 1),
                    sleep: tokio::time::sleep,
                },
                |operation| {
                    let checkpoint = match &operation {
                        ArtifactOperation::Sync { checkpoint, .. }
                        | ArtifactOperation::Observe(checkpoint) => Some(*checkpoint),
                        ArtifactOperation::Write { .. } => None,
                    };
                    if checkpoint == Some(substitution_point) {
                        std::fs::rename(&normalized_part, &normalized_original).unwrap();
                        std::fs::write(&normalized_part, b"UVWXYZ").unwrap();
                        substituted.set(true);
                    }
                    perform_artifact_operation(operation)
                },
            )
            .unwrap_err();

            assert!(substituted.get());
            assert_eq!(failure, DownloadFailure::Durability);
            assert_eq!(&*substitution_transport.offsets.borrow(), &[Some(3)]);
            assert_eq!(substitution_retries.get(), 0);
            assert_eq!(std::fs::read(&normalized_original).unwrap(), b"abcdef");
            assert_eq!(std::fs::read(&normalized_part).unwrap(), b"UVWXYZ");
            assert_eq!(
                std::fs::read(
                    substitution_dir
                        .path()
                        .join("model.gguf.part.restart")
                )
                .unwrap(),
                b"abc"
            );
            assert!(!substitution_dir.path().join("model.gguf").exists());
        }

        let restart_substitution_dir = tempdir().unwrap();
        let restart_part = restart_substitution_dir.path().join("model.gguf.part");
        let restart_candidate = restart_substitution_dir
            .path()
            .join("model.gguf.part.restart");
        let restart_original = restart_substitution_dir.path().join("restart-original");
        std::fs::write(&restart_part, b"abc").unwrap();
        let restart_substitution_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, b"abcdef")]),
            offsets: RefCell::new(Vec::new()),
        };
        let restart_substitution_pause = Cell::new(false);
        let restart_substitution_retries = Cell::new(0);
        let restart_substituted = Cell::new(false);

        let restart_substitution_failure = download_with_transport_controlled(
            &spec(expected),
            restart_substitution_dir.path(),
            &restart_substitution_transport,
            || restart_substitution_pause.get(),
            |update| {
                if matches!(
                    update,
                    ProgressUpdate::Transferring {
                        transferred: 6,
                        ..
                    }
                ) {
                    restart_substitution_pause.set(true);
                }
            },
            RetryWait {
                observer: |_| {
                    restart_substitution_retries
                        .set(restart_substitution_retries.get() + 1);
                },
                sleep: tokio::time::sleep,
            },
            |operation| {
                if matches!(
                    &operation,
                    ArtifactOperation::Observe(ArtifactCheckpoint::RestartIdentityMatched)
                ) {
                    std::fs::rename(&restart_candidate, &restart_original).unwrap();
                    std::fs::write(&restart_candidate, b"XYZ").unwrap();
                    restart_substituted.set(true);
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();

        assert!(restart_substituted.get());
        assert_eq!(restart_substitution_failure, DownloadFailure::Durability);
        assert_eq!(
            &*restart_substitution_transport.offsets.borrow(),
            &[Some(3)]
        );
        assert_eq!(restart_substitution_retries.get(), 0);
        assert_eq!(std::fs::read(&restart_part).unwrap(), b"abcdef");
        assert_eq!(std::fs::read(&restart_candidate).unwrap(), b"XYZ");
        assert_eq!(std::fs::read(&restart_original).unwrap(), b"abc");
        assert!(!restart_substitution_dir.path().join("model.gguf").exists());

        #[derive(Clone, Copy)]
        enum RestartAbsentMutation {
            RecreateRestart,
            SubstitutePart,
            SwapDirectory,
        }

        for mutation in [
            RestartAbsentMutation::RecreateRestart,
            RestartAbsentMutation::SubstitutePart,
            RestartAbsentMutation::SwapDirectory,
        ] {
            let tail_root = tempdir().unwrap();
            let tail_dir = tail_root.path().join("model");
            let moved_tail_dir = tail_root.path().join("moved-model");
            let tail_part = tail_dir.join("model.gguf.part");
            let tail_restart = tail_dir.join("model.gguf.part.restart");
            let tail_original = tail_dir.join("normalized-original");
            std::fs::create_dir(&tail_dir).unwrap();
            std::fs::write(&tail_part, b"abc").unwrap();
            let tail_transport = FakeTransport {
                responses: RefCell::new(vec![transfer(StatusCode::OK, None, b"abcdef")]),
                offsets: RefCell::new(Vec::new()),
            };
            let tail_pause = Cell::new(false);
            let tail_retries = Cell::new(0);
            let tail_mutated = Cell::new(false);

            let tail_failure = download_with_transport_controlled(
                &spec(expected),
                &tail_dir,
                &tail_transport,
                || tail_pause.get(),
                |update| {
                    if matches!(
                        update,
                        ProgressUpdate::Transferring {
                            transferred: 6,
                            ..
                        }
                    ) {
                        tail_pause.set(true);
                    }
                },
                RetryWait {
                    observer: |_| tail_retries.set(tail_retries.get() + 1),
                    sleep: tokio::time::sleep,
                },
                |operation| {
                    if matches!(
                        &operation,
                        ArtifactOperation::Observe(ArtifactCheckpoint::RestartAbsent)
                    ) {
                        match mutation {
                            RestartAbsentMutation::RecreateRestart => {
                                std::fs::write(&tail_restart, b"reborn").unwrap();
                            }
                            RestartAbsentMutation::SubstitutePart => {
                                std::fs::rename(&tail_part, &tail_original).unwrap();
                                std::fs::write(&tail_part, b"UVWXYZ").unwrap();
                            }
                            RestartAbsentMutation::SwapDirectory => {
                                std::fs::rename(&tail_dir, &moved_tail_dir).unwrap();
                                std::fs::create_dir(&tail_dir).unwrap();
                                std::fs::write(tail_dir.join("replacement-witness"), b"current")
                                    .unwrap();
                            }
                        }
                        tail_mutated.set(true);
                    }
                    perform_artifact_operation(operation)
                },
            )
            .unwrap_err();

            assert!(tail_mutated.get());
            assert_eq!(tail_failure, DownloadFailure::Durability);
            assert_eq!(&*tail_transport.offsets.borrow(), &[Some(3)]);
            assert_eq!(tail_retries.get(), 0);
            match mutation {
                RestartAbsentMutation::RecreateRestart => {
                    assert_eq!(std::fs::read(&tail_part).unwrap(), b"abcdef");
                    assert_eq!(std::fs::read(&tail_restart).unwrap(), b"reborn");
                    assert!(!tail_dir.join("model.gguf").exists());
                }
                RestartAbsentMutation::SubstitutePart => {
                    assert_eq!(std::fs::read(&tail_part).unwrap(), b"UVWXYZ");
                    assert_eq!(std::fs::read(&tail_original).unwrap(), b"abcdef");
                    assert!(!tail_restart.exists());
                    assert!(!tail_dir.join("model.gguf").exists());
                }
                RestartAbsentMutation::SwapDirectory => {
                    assert_eq!(
                        std::fs::read(moved_tail_dir.join("model.gguf.part")).unwrap(),
                        b"abcdef"
                    );
                    assert!(!moved_tail_dir.join("model.gguf.part.restart").exists());
                    assert!(!moved_tail_dir.join("model.gguf").exists());
                    assert_eq!(
                        std::fs::read(tail_dir.join("replacement-witness")).unwrap(),
                        b"current"
                    );
                    assert!(!tail_dir.join("model.gguf.part").exists());
                    assert!(!tail_dir.join("model.gguf.part.restart").exists());
                    assert!(!tail_dir.join("model.gguf").exists());
                }
            }
        }
    }

    #[test]
    fn ignored_range_pause_keeps_the_old_part_when_restart_is_shorter_or_not_durable() {
        for (old_part, expected, restart_bytes) in [
            (
                b"abcdef".as_slice(),
                b"abcdefghi".as_slice(),
                b"XYZ".as_slice(),
            ),
            (
                b"abc".as_slice(),
                b"abcdef".as_slice(),
                b"XYZ".as_slice(),
            ),
        ] {
            let dir = tempdir().unwrap();
            std::fs::write(dir.path().join("model.gguf.part"), old_part).unwrap();
            let initial_part = artifact_snapshot(dir.path()).remove(0);
            let transport = FakeTransport {
                responses: RefCell::new(vec![transfer(StatusCode::OK, None, restart_bytes)]),
                offsets: RefCell::new(Vec::new()),
            };
            let pause = Cell::new(false);
            let retries = Cell::new(0);
            let checkpoints = RefCell::new(Vec::new());
            let sync_checkpoints = RefCell::new(Vec::new());

            let outcome = download_with_transport_controlled(
                &spec(expected),
                dir.path(),
                &transport,
                || pause.get(),
                |_| {},
                RetryWait {
                    observer: |_| retries.set(retries.get() + 1),
                    sleep: tokio::time::sleep,
                },
                |operation| {
                    if matches!(
                        &operation,
                        ArtifactOperation::Sync {
                            checkpoint: ArtifactCheckpoint::RecoveredPartSynced,
                            ..
                        }
                    ) {
                        assert_eq!(
                            std::fs::read(dir.path().join("model.gguf.part.restart")).unwrap(),
                            restart_bytes
                        );
                    }
                    match &operation {
                        ArtifactOperation::Sync { checkpoint, .. } => {
                            checkpoints.borrow_mut().push(*checkpoint);
                            sync_checkpoints.borrow_mut().push(*checkpoint);
                        }
                        ArtifactOperation::Observe(checkpoint) => {
                            checkpoints.borrow_mut().push(*checkpoint);
                        }
                        ArtifactOperation::Write { .. } => {}
                    }
                    let wrote = matches!(&operation, ArtifactOperation::Write { .. });
                    let result = perform_artifact_operation(operation);
                    if wrote && result.is_ok() {
                        pause.set(true);
                    }
                    result
                },
            )
            .unwrap();

            assert_eq!(
                outcome,
                DownloadTerminalOutcome::Paused {
                    retained_bytes: old_part.len() as u64,
                }
            );
            assert_eq!(
                &*transport.offsets.borrow(),
                &[Some(old_part.len() as u64)]
            );
            assert_eq!(retries.get(), 0);
            let after_part = artifact_snapshot(dir.path()).remove(0);
            assert_eq!(after_part.name, "model.gguf.part");
            assert_eq!(after_part.bytes, old_part);
            #[cfg(unix)]
            {
                assert_eq!(after_part.device, initial_part.device);
                assert_eq!(after_part.inode, initial_part.inode);
                assert_eq!(after_part.links, initial_part.links);
            }
            assert!(!dir.path().join("model.gguf.part.restart").exists());
            assert!(!dir.path().join("model.gguf").exists());
            assert_eq!(
                &*checkpoints.borrow(),
                &[
                    ArtifactCheckpoint::StagingSynced,
                    ArtifactCheckpoint::StagingIdentityMatched,
                    ArtifactCheckpoint::RecoveredPartSynced,
                    ArtifactCheckpoint::DirectorySynced,
                    ArtifactCheckpoint::BeforeRecoveredPartRestat,
                    ArtifactCheckpoint::BeforeRestartUnlink,
                    ArtifactCheckpoint::RestartIdentityMatched,
                    ArtifactCheckpoint::RestartUnlinked,
                    ArtifactCheckpoint::NormalizationDirectorySynced,
                    ArtifactCheckpoint::BeforeRestartAbsenceProof,
                    ArtifactCheckpoint::RestartAbsent,
                ]
            );
            assert_eq!(
                &*sync_checkpoints.borrow(),
                &[
                    ArtifactCheckpoint::StagingSynced,
                    ArtifactCheckpoint::RecoveredPartSynced,
                    ArtifactCheckpoint::DirectorySynced,
                    ArtifactCheckpoint::NormalizationDirectorySynced,
                ]
            );
        }

        let expected = b"abcdef";
        let zero_dir = tempdir().unwrap();
        std::fs::write(zero_dir.path().join("model.gguf.part"), b"abc").unwrap();
        let zero_pause = std::rc::Rc::new(Cell::new(false));
        let body_pause = std::rc::Rc::clone(&zero_pause);
        let zero_body = std::future::poll_fn(move |_| {
            body_pause.set(true);
            std::task::Poll::Ready(Ok(Some(Vec::new())))
        });
        let zero_transport = FakeTransport {
            responses: RefCell::new(vec![Transfer::test_future(
                StatusCode::OK,
                None,
                zero_body,
            )]),
            offsets: RefCell::new(Vec::new()),
        };
        let zero_retries = Cell::new(0);

        let zero_outcome = download_with_transport_controlled(
            &spec(expected),
            zero_dir.path(),
            &zero_transport,
            || zero_pause.get(),
            |_| {},
            RetryWait {
                observer: |_| zero_retries.set(zero_retries.get() + 1),
                sleep: tokio::time::sleep,
            },
            perform_artifact_operation,
        )
        .unwrap();

        assert_eq!(
            zero_outcome,
            DownloadTerminalOutcome::Paused { retained_bytes: 3 }
        );
        assert_eq!(&*zero_transport.offsets.borrow(), &[Some(3)]);
        assert_eq!(zero_retries.get(), 0);
        assert_eq!(
            std::fs::read(zero_dir.path().join("model.gguf.part")).unwrap(),
            b"abc"
        );
        assert!(!zero_dir.path().join("model.gguf.part.restart").exists());
        assert!(!zero_dir.path().join("model.gguf").exists());

        #[derive(Clone, Copy)]
        enum ShorterFailurePhase {
            BeforeRestartUnlink,
            AfterRestartUnlink,
        }

        let failure_points = [
            (
                ArtifactCheckpoint::StagingSynced,
                ShorterFailurePhase::BeforeRestartUnlink,
            ),
            (
                ArtifactCheckpoint::StagingIdentityMatched,
                ShorterFailurePhase::BeforeRestartUnlink,
            ),
            (
                ArtifactCheckpoint::RecoveredPartSynced,
                ShorterFailurePhase::BeforeRestartUnlink,
            ),
            (
                ArtifactCheckpoint::DirectorySynced,
                ShorterFailurePhase::BeforeRestartUnlink,
            ),
            (
                ArtifactCheckpoint::BeforeRecoveredPartRestat,
                ShorterFailurePhase::BeforeRestartUnlink,
            ),
            (
                ArtifactCheckpoint::BeforeRestartUnlink,
                ShorterFailurePhase::BeforeRestartUnlink,
            ),
            (
                ArtifactCheckpoint::RestartIdentityMatched,
                ShorterFailurePhase::BeforeRestartUnlink,
            ),
            (
                ArtifactCheckpoint::RestartUnlinked,
                ShorterFailurePhase::AfterRestartUnlink,
            ),
            (
                ArtifactCheckpoint::NormalizationDirectorySynced,
                ShorterFailurePhase::AfterRestartUnlink,
            ),
            (
                ArtifactCheckpoint::BeforeRestartAbsenceProof,
                ShorterFailurePhase::AfterRestartUnlink,
            ),
            (
                ArtifactCheckpoint::RestartAbsent,
                ShorterFailurePhase::AfterRestartUnlink,
            ),
        ];

        for (failure_point, phase) in failure_points {
            let failure_dir = tempdir().unwrap();
            std::fs::write(failure_dir.path().join("model.gguf.part"), b"abcdef").unwrap();
            std::fs::write(failure_dir.path().join("foreign.bin"), b"foreign").unwrap();
            std::fs::write(failure_dir.path().join("outside"), b"outside").unwrap();
            let failure_transport = FakeTransport {
                responses: RefCell::new(vec![transfer(StatusCode::OK, None, b"XYZ")]),
                offsets: RefCell::new(Vec::new()),
            };
            let failure_pause = Cell::new(false);
            let failure_retries = Cell::new(0);
            let before_unlink = RefCell::new(None);
            let after_unlink = RefCell::new(None);

            let failure = download_with_transport_controlled(
                &spec(b"abcdefghi"),
                failure_dir.path(),
                &failure_transport,
                || failure_pause.get(),
                |_| {},
                RetryWait {
                    observer: |_| failure_retries.set(failure_retries.get() + 1),
                    sleep: tokio::time::sleep,
                },
                |operation| {
                    let wrote = matches!(&operation, ArtifactOperation::Write { .. });
                    let checkpoint = match &operation {
                        ArtifactOperation::Sync { checkpoint, .. }
                        | ArtifactOperation::Observe(checkpoint) => Some(*checkpoint),
                        ArtifactOperation::Write { .. } => None,
                    };
                    if checkpoint == Some(ArtifactCheckpoint::StagingSynced)
                        && before_unlink.borrow().is_none()
                    {
                        *before_unlink.borrow_mut() =
                            Some(artifact_snapshot(failure_dir.path()));
                    }
                    if checkpoint == Some(ArtifactCheckpoint::RestartUnlinked)
                        && after_unlink.borrow().is_none()
                    {
                        *after_unlink.borrow_mut() = Some(artifact_snapshot(failure_dir.path()));
                    }
                    if checkpoint == Some(failure_point) {
                        return Err(ArtifactOperationFailure::Other);
                    }
                    let result = perform_artifact_operation(operation);
                    if wrote && result.is_ok() {
                        failure_pause.set(true);
                    }
                    result
                },
            )
            .unwrap_err();

            assert_eq!(failure, DownloadFailure::Durability);
            assert_eq!(&*failure_transport.offsets.borrow(), &[Some(6)]);
            assert_eq!(failure_retries.get(), 0);
            assert!(!failure_dir.path().join("model.gguf").exists());
            let after = artifact_snapshot(failure_dir.path());
            let expected_snapshot = match phase {
                ShorterFailurePhase::BeforeRestartUnlink => {
                    before_unlink.borrow().clone().unwrap()
                }
                ShorterFailurePhase::AfterRestartUnlink => {
                    after_unlink.borrow().clone().unwrap()
                }
            };
            assert_eq!(after, expected_snapshot);
            assert_eq!(
                std::fs::read(failure_dir.path().join("model.gguf.part")).unwrap(),
                b"abcdef"
            );
            assert_eq!(
                std::fs::read(failure_dir.path().join("foreign.bin")).unwrap(),
                b"foreign"
            );
            assert_eq!(
                std::fs::read(failure_dir.path().join("outside")).unwrap(),
                b"outside"
            );

            let (directory, _) = crate::safe_file::open_directory(failure_dir.path()).unwrap();
            let sha256 = hex(Sha256::digest(b"abcdefghi").as_ref());
            let plan = match plan::plan_artifact_transfer(
                &directory,
                failure_dir.path(),
                9,
                &sha256,
                &|| false,
            ) {
                plan::ArtifactPlanOutcome::Ready(plan) => plan,
                plan::ArtifactPlanOutcome::Interrupted => {
                    panic!("inert fresh artifact audit interrupted")
                }
            };
            assert_eq!(plan.state(), plan::ArtifactTransferState::RequestCapable);
            assert_eq!(plan.part_length(), Some(6));
            assert_eq!(
                plan.restart_length(),
                match phase {
                    ShorterFailurePhase::BeforeRestartUnlink => Some(3),
                    ShorterFailurePhase::AfterRestartUnlink => None,
                }
            );
        }

        #[cfg(unix)]
        {
            #[derive(Clone, Copy)]
            enum Substitution {
                RestartRegular,
                RestartSymlink,
                RestartHardLink,
                PartRegular,
                PartSymlink,
            }

            for substitution in [
                Substitution::RestartRegular,
                Substitution::RestartSymlink,
                Substitution::RestartHardLink,
                Substitution::PartRegular,
                Substitution::PartSymlink,
            ] {
                let dir = tempdir().unwrap();
                let part = dir.path().join("model.gguf.part");
                let restart = dir.path().join("model.gguf.part.restart");
                let part_original = dir.path().join("part-original");
                let restart_original = dir.path().join("restart-original");
                let outside = dir.path().join("outside-witness");
                std::fs::write(&part, b"abcdef").unwrap();
                std::fs::write(&outside, b"outside").unwrap();
                let initial_part = artifact_snapshot(dir.path())
                    .into_iter()
                    .find(|entry| entry.name == "model.gguf.part")
                    .unwrap();
                let transport = FakeTransport {
                    responses: RefCell::new(vec![transfer(StatusCode::OK, None, b"XYZ")]),
                    offsets: RefCell::new(Vec::new()),
                };
                let pause = Cell::new(false);
                let retries = Cell::new(0);
                let substituted = Cell::new(false);
                let checkpoints = RefCell::new(Vec::new());
                let target_checkpoint = match substitution {
                    Substitution::RestartRegular
                    | Substitution::RestartSymlink
                    | Substitution::RestartHardLink => {
                        ArtifactCheckpoint::StagingIdentityMatched
                    }
                    Substitution::PartRegular | Substitution::PartSymlink => {
                        ArtifactCheckpoint::BeforeRecoveredPartRestat
                    }
                };

                let failure = download_with_transport_controlled(
                    &spec(b"abcdefghi"),
                    dir.path(),
                    &transport,
                    || pause.get(),
                    |_| {},
                    RetryWait {
                        observer: |_| retries.set(retries.get() + 1),
                        sleep: tokio::time::sleep,
                    },
                    |operation| {
                        let wrote = matches!(&operation, ArtifactOperation::Write { .. });
                        let checkpoint = match &operation {
                            ArtifactOperation::Sync { checkpoint, .. }
                            | ArtifactOperation::Observe(checkpoint) => {
                                checkpoints.borrow_mut().push(*checkpoint);
                                Some(*checkpoint)
                            }
                            ArtifactOperation::Write { .. } => None,
                        };
                        if checkpoint == Some(target_checkpoint) {
                            match substitution {
                                Substitution::RestartRegular => {
                                    std::fs::rename(&restart, &restart_original).unwrap();
                                    std::fs::write(&restart, b"QRS").unwrap();
                                }
                                Substitution::RestartSymlink => {
                                    std::fs::rename(&restart, &restart_original).unwrap();
                                    std::os::unix::fs::symlink(&outside, &restart).unwrap();
                                }
                                Substitution::RestartHardLink => {
                                    std::fs::rename(&restart, &restart_original).unwrap();
                                    std::fs::hard_link(&outside, &restart).unwrap();
                                }
                                Substitution::PartRegular => {
                                    std::fs::rename(&part, &part_original).unwrap();
                                    std::fs::write(&part, b"UVWXYZ").unwrap();
                                }
                                Substitution::PartSymlink => {
                                    std::fs::rename(&part, &part_original).unwrap();
                                    std::os::unix::fs::symlink(&outside, &part).unwrap();
                                }
                            }
                            substituted.set(true);
                        }
                        let result = perform_artifact_operation(operation);
                        if wrote && result.is_ok() {
                            pause.set(true);
                        }
                        result
                    },
                )
                .unwrap_err();

                assert!(substituted.get());
                assert_eq!(failure, DownloadFailure::Durability);
                assert_eq!(&*transport.offsets.borrow(), &[Some(6)]);
                assert_eq!(retries.get(), 0);
                assert_eq!(checkpoints.borrow().last(), Some(&target_checkpoint));
                assert!(!checkpoints
                    .borrow()
                    .contains(&ArtifactCheckpoint::BeforeRestartUnlink));
                assert!(!dir.path().join("model.gguf").exists());
                assert_eq!(std::fs::read(&outside).unwrap(), b"outside");
                match substitution {
                    Substitution::RestartRegular => {
                        assert_eq!(std::fs::read(&part).unwrap(), b"abcdef");
                        assert_eq!(std::fs::read(&restart_original).unwrap(), b"XYZ");
                        assert_eq!(std::fs::read(&restart).unwrap(), b"QRS");
                    }
                    Substitution::RestartSymlink | Substitution::RestartHardLink => {
                        assert_eq!(std::fs::read(&part).unwrap(), b"abcdef");
                        assert_eq!(std::fs::read(&restart_original).unwrap(), b"XYZ");
                        assert_eq!(std::fs::read(&restart).unwrap(), b"outside");
                    }
                    Substitution::PartRegular => {
                        assert_eq!(std::fs::read(&part_original).unwrap(), b"abcdef");
                        assert_eq!(std::fs::read(&part).unwrap(), b"UVWXYZ");
                        assert_eq!(std::fs::read(&restart).unwrap(), b"XYZ");
                    }
                    Substitution::PartSymlink => {
                        assert_eq!(std::fs::read(&part_original).unwrap(), b"abcdef");
                        assert_eq!(std::fs::read(&part).unwrap(), b"outside");
                        assert_eq!(std::fs::read(&restart).unwrap(), b"XYZ");
                    }
                }
                if matches!(
                    substitution,
                    Substitution::RestartRegular
                        | Substitution::RestartSymlink
                        | Substitution::RestartHardLink
                ) {
                    let after_part = artifact_snapshot(dir.path())
                        .into_iter()
                        .find(|entry| entry.name == "model.gguf.part")
                        .unwrap();
                    assert_eq!(after_part.device, initial_part.device);
                    assert_eq!(after_part.inode, initial_part.inode);
                    assert_eq!(after_part.links, initial_part.links);
                }
            }

            use std::os::unix::ffi::OsStrExt;
            use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt};

            let fifo_dir = tempdir().unwrap();
            let fifo_part = fifo_dir.path().join("model.gguf.part");
            let fifo_restart = fifo_dir.path().join("model.gguf.part.restart");
            let fifo_original = fifo_dir.path().join("restart-original");
            std::fs::write(&fifo_part, b"abcdef").unwrap();
            let fifo_part_inode = std::fs::metadata(&fifo_part).unwrap().ino();
            let (started_tx, started_rx) = std::sync::mpsc::channel();
            let (result_tx, result_rx) = std::sync::mpsc::channel();
            let worker_dir = fifo_dir.path().to_path_buf();
            let worker_restart = fifo_restart.clone();
            let worker_original = fifo_original.clone();
            let worker = std::thread::spawn(move || {
                let transport = FakeTransport {
                    responses: RefCell::new(vec![transfer(StatusCode::OK, None, b"XYZ")]),
                    offsets: RefCell::new(Vec::new()),
                };
                let pause = Cell::new(false);
                let retries = Cell::new(0);
                let substituted = Cell::new(false);
                let failure = download_with_transport_controlled(
                    &spec(b"abcdefghi"),
                    &worker_dir,
                    &transport,
                    || pause.get(),
                    |_| {},
                    RetryWait {
                        observer: |_| retries.set(retries.get() + 1),
                        sleep: tokio::time::sleep,
                    },
                    |operation| {
                        let wrote = matches!(&operation, ArtifactOperation::Write { .. });
                        if matches!(
                            &operation,
                            ArtifactOperation::Observe(
                                ArtifactCheckpoint::StagingIdentityMatched
                            )
                        ) {
                            std::fs::rename(&worker_restart, &worker_original).unwrap();
                            let path = std::ffi::CString::new(
                                worker_restart.as_os_str().as_bytes(),
                            )
                            .unwrap();
                            assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
                            substituted.set(true);
                            let _ = started_tx.send(());
                        }
                        let result = perform_artifact_operation(operation);
                        if wrote && result.is_ok() {
                            pause.set(true);
                        }
                        result
                    },
                )
                .unwrap_err();
                let _ = result_tx.send((
                    failure,
                    retries.get(),
                    transport.offsets.into_inner(),
                    substituted.get(),
                ));
            });

            if let Err(error) = started_rx.recv_timeout(std::time::Duration::from_secs(2)) {
                match result_rx.recv_timeout(std::time::Duration::from_secs(2)) {
                    Ok(_) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                        worker.join().unwrap();
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => drop(worker),
                }
                panic!("FIFO restart substitution did not reach its handshake: {error:?}");
            }
            let first = result_rx.recv_timeout(std::time::Duration::from_millis(250));
            let (returned_without_release, fifo_result) = match first {
                Ok(result) => (true, result),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    let release = std::fs::OpenOptions::new()
                        .write(true)
                        .custom_flags(libc::O_NONBLOCK)
                        .open(&fifo_restart)
                        .expect("failed to release blocked restart FIFO reader");
                    let result = result_rx
                        .recv_timeout(std::time::Duration::from_secs(2))
                        .expect("released restart FIFO worker must finish");
                    drop(release);
                    (false, result)
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    worker.join().unwrap();
                    panic!("restart FIFO worker disconnected")
                }
            };
            worker.join().unwrap();

            assert!(returned_without_release);
            assert_eq!(fifo_result.0, DownloadFailure::Durability);
            assert_eq!(fifo_result.1, 0);
            assert_eq!(fifo_result.2, [Some(6)]);
            assert!(fifo_result.3);
            assert_eq!(std::fs::read(&fifo_part).unwrap(), b"abcdef");
            assert_eq!(std::fs::metadata(&fifo_part).unwrap().ino(), fifo_part_inode);
            assert_eq!(std::fs::read(&fifo_original).unwrap(), b"XYZ");
            assert!(std::fs::metadata(&fifo_restart)
                .unwrap()
                .file_type()
                .is_fifo());
            assert!(!fifo_dir.path().join("model.gguf").exists());

            let swap_root = tempdir().unwrap();
            let swap_dir = swap_root.path().join("model");
            let moved_swap_dir = swap_root.path().join("moved-model");
            std::fs::create_dir(&swap_dir).unwrap();
            std::fs::write(swap_dir.join("model.gguf.part"), b"abcdef").unwrap();
            let swap_transport = FakeTransport {
                responses: RefCell::new(vec![transfer(StatusCode::OK, None, b"XYZ")]),
                offsets: RefCell::new(Vec::new()),
            };
            let swap_pause = Cell::new(false);
            let swap_retries = Cell::new(0);
            let swapped = Cell::new(false);
            let swap_failure = download_with_transport_controlled(
                &spec(b"abcdefghi"),
                &swap_dir,
                &swap_transport,
                || swap_pause.get(),
                |_| {},
                RetryWait {
                    observer: |_| swap_retries.set(swap_retries.get() + 1),
                    sleep: tokio::time::sleep,
                },
                |operation| {
                    let wrote = matches!(&operation, ArtifactOperation::Write { .. });
                    if matches!(
                        &operation,
                        ArtifactOperation::Observe(ArtifactCheckpoint::RestartIdentityMatched)
                    ) {
                        std::fs::rename(&swap_dir, &moved_swap_dir).unwrap();
                        std::fs::create_dir(&swap_dir).unwrap();
                        std::fs::write(
                            swap_dir.join("model.gguf.part.restart"),
                            b"replacement",
                        )
                        .unwrap();
                        std::fs::write(swap_dir.join("outside-witness"), b"current").unwrap();
                        swapped.set(true);
                    }
                    let result = perform_artifact_operation(operation);
                    if wrote && result.is_ok() {
                        swap_pause.set(true);
                    }
                    result
                },
            )
            .unwrap_err();

            assert!(swapped.get());
            assert_eq!(swap_failure, DownloadFailure::Durability);
            assert_eq!(&*swap_transport.offsets.borrow(), &[Some(6)]);
            assert_eq!(swap_retries.get(), 0);
            assert_eq!(
                std::fs::read(moved_swap_dir.join("model.gguf.part")).unwrap(),
                b"abcdef"
            );
            assert_eq!(
                std::fs::read(moved_swap_dir.join("model.gguf.part.restart")).unwrap(),
                b"XYZ"
            );
            assert!(!moved_swap_dir.join("model.gguf").exists());
            assert_eq!(
                std::fs::read(swap_dir.join("model.gguf.part.restart")).unwrap(),
                b"replacement"
            );
            assert_eq!(
                std::fs::read(swap_dir.join("outside-witness")).unwrap(),
                b"current"
            );
            assert!(!swap_dir.join("model.gguf.part").exists());
            assert!(!swap_dir.join("model.gguf").exists());

        }
    }

    #[test]
    fn terminal_error_normalizes_the_longest_exact_prefix_for_the_next_range_offset() {
        for (old_part, expected, restart, normalized, short_eof) in [
            (
                b"abc".as_slice(),
                b"abcdefghi".as_slice(),
                b"abcdef".as_slice(),
                b"abcdef".as_slice(),
                false,
            ),
            (
                b"abcdef".as_slice(),
                b"abcdefghi".as_slice(),
                b"XYZ".as_slice(),
                b"abcdef".as_slice(),
                false,
            ),
            (
                b"abc".as_slice(),
                b"abcdef".as_slice(),
                b"XYZ".as_slice(),
                b"abc".as_slice(),
                false,
            ),
            (
                b"abc".as_slice(),
                b"abcdef".as_slice(),
                b"".as_slice(),
                b"abc".as_slice(),
                false,
            ),
            (
                b"abc".as_slice(),
                b"abcdefghi".as_slice(),
                b"abcdef".as_slice(),
                b"abcdef".as_slice(),
                true,
            ),
            (
                b"abcdef".as_slice(),
                b"abcdefghi".as_slice(),
                b"XYZ".as_slice(),
                b"abcdef".as_slice(),
                true,
            ),
        ] {
            let dir = tempdir().unwrap();
            std::fs::write(dir.path().join("model.gguf.part"), old_part).unwrap();
            let transport = FakeTransport {
                responses: RefCell::new(vec![
                    if short_eof {
                        transfer(StatusCode::OK, None, restart)
                    } else {
                        failing_transfer(StatusCode::OK, None, restart)
                    },
                    transfer(StatusCode::BAD_REQUEST, None, b""),
                ]),
                offsets: RefCell::new(Vec::new()),
            };
            let retries = Cell::new(0);
            let checkpoints = RefCell::new(Vec::new());

            let failure = download_with_transport_controlled(
                &spec(expected),
                dir.path(),
                &transport,
                || false,
                |_| {},
                RetryWait {
                    observer: |_| retries.set(retries.get() + 1),
                    sleep: |_| std::future::ready(()),
                },
                |operation| {
                    match &operation {
                        ArtifactOperation::Sync { checkpoint, .. }
                        | ArtifactOperation::Observe(checkpoint) => {
                            checkpoints.borrow_mut().push(*checkpoint)
                        }
                        ArtifactOperation::Write { .. } => {}
                    }
                    perform_artifact_operation(operation)
                },
            )
            .unwrap_err();

            let retained_bytes = normalized.len() as u64;
            assert_eq!(failure, DownloadFailure::Remote { retained_bytes });
            assert_eq!(
                &*transport.offsets.borrow(),
                &[Some(old_part.len() as u64), Some(retained_bytes)]
            );
            assert_eq!(retries.get(), 1);
            let mut expected_checkpoints = if restart.len() > old_part.len() {
                vec![
                    ArtifactCheckpoint::StagingSynced,
                    ArtifactCheckpoint::StagingIdentityMatched,
                    ArtifactCheckpoint::BeforePrefixExchange,
                    ArtifactCheckpoint::PrefixExchanged,
                    ArtifactCheckpoint::DirectorySynced,
                    ArtifactCheckpoint::AuthoritativeRestatted,
                    ArtifactCheckpoint::BeforeRestartUnlink,
                    ArtifactCheckpoint::RestartIdentityMatched,
                    ArtifactCheckpoint::RestartUnlinked,
                    ArtifactCheckpoint::NormalizationDirectorySynced,
                    ArtifactCheckpoint::BeforeRestartAbsenceProof,
                    ArtifactCheckpoint::RestartAbsent,
                ]
            } else {
                vec![
                    ArtifactCheckpoint::StagingSynced,
                    ArtifactCheckpoint::StagingIdentityMatched,
                    ArtifactCheckpoint::RecoveredPartSynced,
                    ArtifactCheckpoint::DirectorySynced,
                    ArtifactCheckpoint::BeforeRecoveredPartRestat,
                    ArtifactCheckpoint::BeforeRestartUnlink,
                    ArtifactCheckpoint::RestartIdentityMatched,
                    ArtifactCheckpoint::RestartUnlinked,
                    ArtifactCheckpoint::NormalizationDirectorySynced,
                    ArtifactCheckpoint::BeforeRestartAbsenceProof,
                    ArtifactCheckpoint::RestartAbsent,
                ]
            };
            expected_checkpoints.extend([
                ArtifactCheckpoint::StagingSynced,
                ArtifactCheckpoint::StagingIdentityMatched,
                ArtifactCheckpoint::DirectorySynced,
                ArtifactCheckpoint::NormalizationDirectorySynced,
                ArtifactCheckpoint::AuthoritativeRestatted,
            ]);
            assert_eq!(&*checkpoints.borrow(), &expected_checkpoints);
            assert_eq!(
                std::fs::read(dir.path().join("model.gguf.part")).unwrap(),
                normalized
            );
            assert!(!dir.path().join("model.gguf.part.restart").exists());
            assert!(!dir.path().join("model.gguf").exists());

            let (directory, _) = crate::safe_file::open_directory(dir.path()).unwrap();
            let sha256 = hex(Sha256::digest(expected).as_ref());
            let plan = match plan::plan_artifact_transfer(
                &directory,
                dir.path(),
                expected.len() as u64,
                &sha256,
                &|| false,
            ) {
                plan::ArtifactPlanOutcome::Ready(plan) => plan,
                plan::ArtifactPlanOutcome::Interrupted => {
                    panic!("inert fresh artifact audit interrupted")
                }
            };
            assert_eq!(plan.state(), plan::ArtifactTransferState::RequestCapable);
            assert_eq!(plan.part_length(), Some(retained_bytes));
            assert_eq!(plan.restart_length(), None);

            let next_transport = FailingTransport {
                remaining_failures: Cell::new(1),
                retryable: false,
                attempts: Cell::new(0),
                offsets: RefCell::new(Vec::new()),
                bytes: Vec::new(),
            };
            let next_retries = Cell::new(0);
            let next_failure = download_with_transport_controlled(
                &spec(expected),
                dir.path(),
                &next_transport,
                || false,
                |_| {},
                RetryWait {
                    observer: |_| next_retries.set(next_retries.get() + 1),
                    sleep: |_| std::future::ready(()),
                },
                perform_artifact_operation,
            )
            .unwrap_err();

            assert_eq!(
                next_failure,
                DownloadFailure::Remote { retained_bytes }
            );
            assert_eq!(next_transport.attempts.get(), 1);
            assert_eq!(&*next_transport.offsets.borrow(), &[Some(retained_bytes)]);
            assert_eq!(next_retries.get(), 0);
            assert_eq!(
                std::fs::read(dir.path().join("model.gguf.part")).unwrap(),
                normalized
            );
            assert!(!dir.path().join("model.gguf.part.restart").exists());
            assert!(!dir.path().join("model.gguf").exists());
        }

        let replacement_dir = tempdir().unwrap();
        let replacement_part = replacement_dir.path().join("model.gguf.part");
        let replaced_part = replacement_dir.path().join("normalized-before-replacement");
        std::fs::write(&replacement_part, b"abc").unwrap();
        let replacement_transport = FakeTransport {
            responses: RefCell::new(vec![
                failing_transfer(StatusCode::OK, None, b"abcdef"),
                transfer(StatusCode::BAD_REQUEST, None, b""),
            ]),
            offsets: RefCell::new(Vec::new()),
        };
        let replacement_retries = Cell::new(0);
        let replaced = Cell::new(false);
        let replacement_failure = download_with_transport_controlled(
            &spec(b"abcdefghi"),
            replacement_dir.path(),
            &replacement_transport,
            || false,
            |_| {},
            RetryWait {
                observer: |_| replacement_retries.set(replacement_retries.get() + 1),
                sleep: |_| {
                    if !replaced.replace(true) {
                        std::fs::rename(&replacement_part, &replaced_part).unwrap();
                        std::fs::write(&replacement_part, b"WXYZ").unwrap();
                    }
                    std::future::ready(())
                },
            },
            perform_artifact_operation,
        )
        .unwrap_err();
        assert_eq!(
            replacement_failure,
            DownloadFailure::Remote { retained_bytes: 4 }
        );
        assert_eq!(&*replacement_transport.offsets.borrow(), &[Some(3), Some(4)]);
        assert_eq!(replacement_retries.get(), 1);
        assert_eq!(std::fs::read(&replacement_part).unwrap(), b"WXYZ");
        assert_eq!(std::fs::read(&replaced_part).unwrap(), b"abcdef");

        for mutation_kind in 0..3 {
            let root = tempdir().unwrap();
            let model_dir = root.path().join("model");
            let moved_dir = root.path().join("moved");
            std::fs::create_dir(&model_dir).unwrap();
            let part = model_dir.join("model.gguf.part");
            let original = model_dir.join("normalized-original");
            std::fs::write(&part, b"abc").unwrap();
            let mutation = match mutation_kind {
                0 => SecondRequestMutation::Part {
                    part: part.clone(),
                    original: original.clone(),
                    replacement: b"UVWXYZ".to_vec(),
                },
                1 => SecondRequestMutation::InPlacePart {
                    part: part.clone(),
                    replacement: b"UVWXYZ".to_vec(),
                },
                _ => SecondRequestMutation::Directory {
                    model_dir: model_dir.clone(),
                    moved_dir: moved_dir.clone(),
                },
            };
            let transport = BodyThenMutatingProtocolTransport {
                offsets: RefCell::new(Vec::new()),
                first: RefCell::new(Some(failing_transfer(
                    StatusCode::OK,
                    None,
                    b"abcdef",
                ))),
                mutation: RefCell::new(Some(mutation)),
            };
            let retries = Cell::new(0);
            let failure = download_with_transport_controlled(
                &spec(b"abcdefghi"),
                &model_dir,
                &transport,
                || false,
                |_| {},
                RetryWait {
                    observer: |_| retries.set(retries.get() + 1),
                    sleep: |_| std::future::ready(()),
                },
                perform_artifact_operation,
            )
            .unwrap_err();

            assert_eq!(failure, DownloadFailure::Durability);
            assert_eq!(&*transport.offsets.borrow(), &[Some(3), Some(6)]);
            assert_eq!(retries.get(), 1);
            if mutation_kind == 2 {
                assert_eq!(
                    std::fs::read(moved_dir.join("model.gguf.part")).unwrap(),
                    b"abcdef"
                );
                assert_eq!(
                    std::fs::read(model_dir.join("foreign.bin")).unwrap(),
                    b"replacement foreign"
                );
                assert!(!model_dir.join("model.gguf").exists());
            } else if mutation_kind == 0 {
                assert_eq!(std::fs::read(&original).unwrap(), b"abcdef");
                assert_eq!(std::fs::read(&part).unwrap(), b"UVWXYZ");
                assert!(!model_dir.join("model.gguf").exists());
            } else {
                assert_eq!(std::fs::read(&part).unwrap(), b"UVWXYZ");
                assert!(!original.exists());
                assert!(!model_dir.join("model.gguf").exists());
            }
        }

        let proof_mutation_dir = tempdir().unwrap();
        let proof_mutation_part = proof_mutation_dir.path().join("model.gguf.part");
        std::fs::write(&proof_mutation_part, b"abc").unwrap();
        let proof_mutation_transport = FakeTransport {
            responses: RefCell::new(vec![
                failing_transfer(StatusCode::OK, None, b"abcdef"),
                transfer(StatusCode::BAD_REQUEST, None, b""),
            ]),
            offsets: RefCell::new(Vec::new()),
        };
        let proof_mutation_retries = Cell::new(0);
        let staging_syncs = Cell::new(0);
        let proof_mutation_failure = download_with_transport_controlled(
            &spec(b"abcdefghi"),
            proof_mutation_dir.path(),
            &proof_mutation_transport,
            || false,
            |_| {},
            RetryWait {
                observer: |_| {
                    proof_mutation_retries.set(proof_mutation_retries.get() + 1)
                },
                sleep: |_| std::future::ready(()),
            },
            |operation| {
                let staging_sync = matches!(
                    &operation,
                    ArtifactOperation::Sync {
                        checkpoint: ArtifactCheckpoint::StagingSynced,
                        ..
                    }
                );
                perform_artifact_operation(operation)?;
                if staging_sync {
                    let next = staging_syncs.get() + 1;
                    staging_syncs.set(next);
                    if next == 2 {
                        std::fs::write(&proof_mutation_part, b"UVWXYZ").unwrap();
                    }
                }
                Ok(())
            },
        )
        .unwrap_err();
        assert_eq!(proof_mutation_failure, DownloadFailure::Durability);
        assert_eq!(
            &*proof_mutation_transport.offsets.borrow(),
            &[Some(3), Some(6)]
        );
        assert_eq!(proof_mutation_retries.get(), 1);
        assert_eq!(staging_syncs.get(), 2);
        assert_eq!(std::fs::read(&proof_mutation_part).unwrap(), b"UVWXYZ");
        assert!(!proof_mutation_dir.path().join("model.gguf").exists());

        let missing_dir = tempdir().unwrap();
        let missing_part = missing_dir.path().join("model.gguf.part");
        let removed_part = missing_dir.path().join("removed-part");
        std::fs::write(&missing_part, b"abc").unwrap();
        let missing_transport = FakeTransport {
            responses: RefCell::new(vec![
                failing_transfer(StatusCode::OK, None, b"abcdef"),
                transfer(StatusCode::BAD_REQUEST, None, b""),
            ]),
            offsets: RefCell::new(Vec::new()),
        };
        let missing_retries = Cell::new(0);
        let missing_failure = download_with_transport_controlled(
            &spec(b"abcdefghi"),
            missing_dir.path(),
            &missing_transport,
            || false,
            |_| {},
            RetryWait {
                observer: |_| missing_retries.set(missing_retries.get() + 1),
                sleep: |_| {
                    std::fs::rename(&missing_part, &removed_part).unwrap();
                    std::future::ready(())
                },
            },
            perform_artifact_operation,
        )
        .unwrap_err();
        assert_eq!(missing_failure, DownloadFailure::Durability);
        assert_eq!(&*missing_transport.offsets.borrow(), &[Some(3)]);
        assert_eq!(missing_retries.get(), 1);
        assert_eq!(std::fs::read(&removed_part).unwrap(), b"abcdef");
        assert!(!missing_part.exists());

        #[cfg(unix)]
        {
            let unsafe_dir = tempdir().unwrap();
            let unsafe_part = unsafe_dir.path().join("model.gguf.part");
            let prior = unsafe_dir.path().join("prior-part");
            let outside = unsafe_dir.path().join("outside");
            std::fs::write(&unsafe_part, b"abc").unwrap();
            std::fs::write(&outside, b"outside").unwrap();
            let transport = FakeTransport {
                responses: RefCell::new(vec![
                    failing_transfer(StatusCode::OK, None, b"abcdef"),
                    transfer(StatusCode::BAD_REQUEST, None, b""),
                ]),
                offsets: RefCell::new(Vec::new()),
            };
            let retries = Cell::new(0);
            let failure = download_with_transport_controlled(
                &spec(b"abcdefghi"),
                unsafe_dir.path(),
                &transport,
                || false,
                |_| {},
                RetryWait {
                    observer: |_| retries.set(retries.get() + 1),
                    sleep: |_| {
                        std::fs::rename(&unsafe_part, &prior).unwrap();
                        std::os::unix::fs::symlink(&outside, &unsafe_part).unwrap();
                        std::future::ready(())
                    },
                },
                perform_artifact_operation,
            )
            .unwrap_err();
            assert_eq!(failure, DownloadFailure::Durability);
            assert_eq!(&*transport.offsets.borrow(), &[Some(3)]);
            assert_eq!(retries.get(), 1);
            assert_eq!(std::fs::read(&prior).unwrap(), b"abcdef");
            assert_eq!(std::fs::read(&outside).unwrap(), b"outside");
            assert!(std::fs::symlink_metadata(&unsafe_part)
                .unwrap()
                .file_type()
                .is_symlink());
            assert!(!unsafe_dir.path().join("model.gguf").exists());
        }

        #[cfg(unix)]
        {
            let root = tempdir().unwrap();
            let model_dir = root.path().join("model");
            let moved_dir = root.path().join("moved");
            std::fs::create_dir(&model_dir).unwrap();
            std::fs::write(model_dir.join("model.gguf.part"), b"abc").unwrap();
            let transport = FakeTransport {
                responses: RefCell::new(vec![
                    failing_transfer(StatusCode::OK, None, b"abcdef"),
                    transfer(StatusCode::BAD_REQUEST, None, b""),
                ]),
                offsets: RefCell::new(Vec::new()),
            };
            let retries = Cell::new(0);
            let failure = download_with_transport_controlled(
                &spec(b"abcdefghi"),
                &model_dir,
                &transport,
                || false,
                |_| {},
                RetryWait {
                    observer: |_| retries.set(retries.get() + 1),
                    sleep: |_| {
                        std::fs::rename(&model_dir, &moved_dir).unwrap();
                        std::os::unix::fs::symlink(&moved_dir, &model_dir).unwrap();
                        std::future::ready(())
                    },
                },
                perform_artifact_operation,
            )
            .unwrap_err();
            assert_eq!(failure, DownloadFailure::Durability);
            assert_eq!(&*transport.offsets.borrow(), &[Some(3)]);
            assert_eq!(retries.get(), 1);
            assert!(std::fs::symlink_metadata(&model_dir)
                .unwrap()
                .file_type()
                .is_symlink());
            assert_eq!(
                std::fs::read(moved_dir.join("model.gguf.part")).unwrap(),
                b"abcdef"
            );
            assert!(!moved_dir.join("model.gguf").exists());
        }

        for (
            initial,
            first_prefix,
            replacement,
            expected_retained,
            expected_offset,
            mutate_during_proof,
        ) in [
            (
                Some(b"abc".as_slice()),
                b"abcdef".as_slice(),
                Some(b"WXYZ".as_slice()),
                4,
                Some(3),
                false,
            ),
            (None, b"".as_slice(), None, 0, None, false),
            (
                Some(b"abc".as_slice()),
                b"abcdef".as_slice(),
                None,
                6,
                Some(3),
                true,
            ),
        ] {
            let dir = tempdir().unwrap();
            let part = dir.path().join("model.gguf.part");
            let prior = dir.path().join("prior-part");
            if let Some(initial) = initial {
                std::fs::write(&part, initial).unwrap();
            }
            let transport = FakeTransport {
                responses: RefCell::new(vec![failing_transfer(
                    StatusCode::OK,
                    None,
                    first_prefix,
                )]),
                offsets: RefCell::new(Vec::new()),
            };
            let pause = Cell::new(false);
            let retries = Cell::new(0);
            let restats = Cell::new(0);
            let staging_syncs = Cell::new(0);
            let result = download_with_transport_controlled(
                &spec(b"abcdefghi"),
                dir.path(),
                &transport,
                || pause.get(),
                |_| {},
                RetryWait {
                    observer: |_| retries.set(retries.get() + 1),
                    sleep: |_| {
                        if let Some(replacement) = replacement {
                            std::fs::rename(&part, &prior).unwrap();
                            std::fs::write(&part, replacement).unwrap();
                        }
                        pause.set(true);
                        std::future::ready(())
                    },
                },
                |operation| {
                    let staging_sync = matches!(
                        &operation,
                        ArtifactOperation::Sync {
                            checkpoint: ArtifactCheckpoint::StagingSynced,
                            ..
                        }
                    );
                    if matches!(
                        &operation,
                        ArtifactOperation::Observe(ArtifactCheckpoint::AuthoritativeRestatted)
                    ) {
                        restats.set(restats.get() + 1);
                    }
                    perform_artifact_operation(operation)?;
                    if staging_sync {
                        let next = staging_syncs.get() + 1;
                        staging_syncs.set(next);
                        if mutate_during_proof && next == 2 {
                            std::fs::write(&part, b"UVWXYZ").unwrap();
                        }
                    }
                    Ok(())
                },
            );

            if mutate_during_proof {
                assert_eq!(result.unwrap_err(), DownloadFailure::Durability);
                assert_eq!(std::fs::read(&part).unwrap(), b"UVWXYZ");
            } else {
                assert_eq!(
                    result.unwrap(),
                    DownloadTerminalOutcome::Paused {
                        retained_bytes: expected_retained,
                    }
                );
            }
            assert_eq!(&*transport.offsets.borrow(), &[expected_offset]);
            assert_eq!(retries.get(), 1);
            assert_eq!(restats.get(), 2);
            assert_eq!(
                std::fs::metadata(&part).unwrap().len(),
                expected_retained
            );
            assert!(!dir.path().join("model.gguf").exists());
            assert!(!dir.path().join("model.gguf.part.restart").exists());
        }

        for final_appears in [true, false] {
            let dir = tempdir().unwrap();
            let part = dir.path().join("model.gguf.part");
            let final_path = dir.path().join("model.gguf");
            let invalid_path = dir.path().join("model.gguf.invalid");
            let restart_path = dir.path().join("model.gguf.part.restart");
            std::fs::write(&part, b"abc").unwrap();
            let transport = FakeTransport {
                responses: RefCell::new(vec![failing_transfer(
                    StatusCode::OK,
                    None,
                    b"abcdef",
                )]),
                offsets: RefCell::new(Vec::new()),
            };
            let pause = Cell::new(false);
            let retries = Cell::new(0);
            let result = download_with_transport_controlled(
                &spec(b"abcdefghi"),
                dir.path(),
                &transport,
                || pause.get(),
                |_| {},
                RetryWait {
                    observer: |_| retries.set(retries.get() + 1),
                    sleep: |_| {
                        if final_appears {
                            std::fs::write(&final_path, b"abcdefghi").unwrap();
                            std::fs::write(&invalid_path, b"repair evidence").unwrap();
                            std::fs::write(&restart_path, b"candidate evidence").unwrap();
                        } else {
                            std::fs::write(&part, b"abcdefghij").unwrap();
                        }
                        pause.set(true);
                        std::future::ready(())
                    },
                },
                perform_artifact_operation,
            );

            assert_eq!(&*transport.offsets.borrow(), &[Some(3)]);
            assert_eq!(retries.get(), 1);
            if final_appears {
                assert_eq!(
                    result.unwrap(),
                    DownloadTerminalOutcome::Paused { retained_bytes: 6 }
                );
                assert_eq!(std::fs::read(&part).unwrap(), b"abcdef");
                assert_eq!(std::fs::read(&final_path).unwrap(), b"abcdefghi");
                assert_eq!(std::fs::read(&invalid_path).unwrap(), b"repair evidence");
                assert_eq!(
                    std::fs::read(&restart_path).unwrap(),
                    b"candidate evidence"
                );
            } else {
                assert_eq!(result.unwrap_err(), DownloadFailure::Durability);
                assert_eq!(std::fs::read(&part).unwrap(), b"abcdefghij");
                assert!(!final_path.exists());
                assert!(!invalid_path.exists());
                assert!(!restart_path.exists());
            }
        }

        for (old_part, expected, response, write_len, retained_bytes) in [
            (
                b"abc".as_slice(),
                b"abcdefghi".as_slice(),
                b"abcdef".as_slice(),
                6,
                6,
            ),
            (
                b"abcdef".as_slice(),
                b"abcdefghi".as_slice(),
                b"XYZ".as_slice(),
                3,
                6,
            ),
        ] {
            let dir = tempdir().unwrap();
            std::fs::write(dir.path().join("model.gguf.part"), old_part).unwrap();
            let transport = FakeTransport {
                responses: RefCell::new(vec![transfer(StatusCode::OK, None, response)]),
                offsets: RefCell::new(Vec::new()),
            };
            let retries = Cell::new(0);
            let failure = download_with_transport_controlled(
                &spec(expected),
                dir.path(),
                &transport,
                || false,
                |_| {},
                RetryWait {
                    observer: |_| retries.set(retries.get() + 1),
                    sleep: |_| std::future::ready(()),
                },
                |operation| match operation {
                    ArtifactOperation::Write { file, bytes } => {
                        file.write_all(&bytes[..write_len]).unwrap();
                        Err(ArtifactOperationFailure::DiskExhausted)
                    }
                    operation => perform_artifact_operation(operation),
                },
            )
            .unwrap_err();

            assert_eq!(
                failure,
                DownloadFailure::DiskExhausted { retained_bytes }
            );
            assert_eq!(&*transport.offsets.borrow(), &[Some(old_part.len() as u64)]);
            assert_eq!(retries.get(), 0);
            assert_eq!(
                std::fs::metadata(dir.path().join("model.gguf.part"))
                    .unwrap()
                    .len(),
                retained_bytes
            );
            assert_eq!(
                std::fs::read(dir.path().join("model.gguf.part")).unwrap(),
                b"abcdef"
            );
            assert!(!dir.path().join("model.gguf.part.restart").exists());
            assert!(!dir.path().join("model.gguf").exists());
        }

        let enospc_failure_dir = tempdir().unwrap();
        std::fs::write(
            enospc_failure_dir.path().join("model.gguf.part"),
            b"abc",
        )
        .unwrap();
        let enospc_failure_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, b"abcdef")]),
            offsets: RefCell::new(Vec::new()),
        };
        let enospc_failure = download_with_transport_controlled(
            &spec(b"abcdefghi"),
            enospc_failure_dir.path(),
            &enospc_failure_transport,
            || false,
            |_| {},
            RetryWait {
                observer: |_| panic!("local disk exhaustion must not retry"),
                sleep: |_| std::future::ready(()),
            },
            |operation| match operation {
                ArtifactOperation::Write { file, bytes } => {
                    file.write_all(bytes).unwrap();
                    Err(ArtifactOperationFailure::DiskExhausted)
                }
                ArtifactOperation::Sync {
                    checkpoint: ArtifactCheckpoint::StagingSynced,
                    ..
                } => Err(ArtifactOperationFailure::Other),
                operation => perform_artifact_operation(operation),
            },
        )
        .unwrap_err();
        assert_eq!(enospc_failure, DownloadFailure::Durability);
        assert_eq!(&*enospc_failure_transport.offsets.borrow(), &[Some(3)]);
        assert_eq!(
            std::fs::read(enospc_failure_dir.path().join("model.gguf.part")).unwrap(),
            b"abc"
        );
        assert_eq!(
            std::fs::read(
                enospc_failure_dir
                    .path()
                    .join("model.gguf.part.restart")
            )
            .unwrap(),
            b"abcdef"
        );
        assert!(!enospc_failure_dir.path().join("model.gguf").exists());

        for (old_part, expected, restart, failure_point, part_after, restart_after) in [
            (
                b"abc".as_slice(),
                b"abcdefghi".as_slice(),
                b"abcdef".as_slice(),
                ArtifactCheckpoint::StagingSynced,
                b"abc".as_slice(),
                Some(b"abcdef".as_slice()),
            ),
            (
                b"abcdef".as_slice(),
                b"abcdefghi".as_slice(),
                b"XYZ".as_slice(),
                ArtifactCheckpoint::RecoveredPartSynced,
                b"abcdef".as_slice(),
                Some(b"XYZ".as_slice()),
            ),
        ] {
            let dir = tempdir().unwrap();
            std::fs::write(dir.path().join("model.gguf.part"), old_part).unwrap();
            let transport = FakeTransport {
                responses: RefCell::new(vec![failing_transfer(
                    StatusCode::OK,
                    None,
                    restart,
                )]),
                offsets: RefCell::new(Vec::new()),
            };
            let retries = Cell::new(0);
            let failure = download_with_transport_controlled(
                &spec(expected),
                dir.path(),
                &transport,
                || false,
                |_| {},
                RetryWait {
                    observer: |_| retries.set(retries.get() + 1),
                    sleep: |_| std::future::ready(()),
                },
                |operation| {
                    let checkpoint = match &operation {
                        ArtifactOperation::Sync { checkpoint, .. }
                        | ArtifactOperation::Observe(checkpoint) => Some(*checkpoint),
                        ArtifactOperation::Write { .. } => None,
                    };
                    if checkpoint == Some(failure_point) {
                        return Err(ArtifactOperationFailure::Other);
                    }
                    perform_artifact_operation(operation)
                },
            )
            .unwrap_err();

            assert_eq!(failure, DownloadFailure::Durability);
            assert_eq!(&*transport.offsets.borrow(), &[Some(old_part.len() as u64)]);
            assert_eq!(retries.get(), 0);
            assert_eq!(
                std::fs::read(dir.path().join("model.gguf.part")).unwrap(),
                part_after
            );
            let restart_path = dir.path().join("model.gguf.part.restart");
            match restart_after {
                Some(restart_after) => {
                    assert_eq!(std::fs::read(restart_path).unwrap(), restart_after)
                }
                None => assert!(!restart_path.exists()),
            }
            assert!(!dir.path().join("model.gguf").exists());
        }
    }

    #[test]
    fn normalization_crash_reentry_never_loses_both_prefixes_or_claims_unproven_bytes() {
        #[derive(Clone, Copy)]
        enum CrashPhase {
            BeforeExchange,
            AfterExchange,
            PartOnly,
        }

        let failure_points = [
            (ArtifactCheckpoint::StagingSynced, CrashPhase::BeforeExchange),
            (
                ArtifactCheckpoint::StagingIdentityMatched,
                CrashPhase::BeforeExchange,
            ),
            (
                ArtifactCheckpoint::BeforePrefixExchange,
                CrashPhase::BeforeExchange,
            ),
            (ArtifactCheckpoint::PrefixExchanged, CrashPhase::AfterExchange),
            (ArtifactCheckpoint::DirectorySynced, CrashPhase::AfterExchange),
            (
                ArtifactCheckpoint::AuthoritativeRestatted,
                CrashPhase::AfterExchange,
            ),
            (
                ArtifactCheckpoint::BeforeRestartUnlink,
                CrashPhase::AfterExchange,
            ),
            (
                ArtifactCheckpoint::RestartIdentityMatched,
                CrashPhase::AfterExchange,
            ),
            (ArtifactCheckpoint::RestartUnlinked, CrashPhase::PartOnly),
            (
                ArtifactCheckpoint::NormalizationDirectorySynced,
                CrashPhase::PartOnly,
            ),
            (
                ArtifactCheckpoint::BeforeRestartAbsenceProof,
                CrashPhase::PartOnly,
            ),
            (ArtifactCheckpoint::RestartAbsent, CrashPhase::PartOnly),
        ];

        for (failure_point, phase) in failure_points {
            let dir = tempdir().unwrap();
            let part = dir.path().join("model.gguf.part");
            let restart = dir.path().join("model.gguf.part.restart");
            std::fs::write(&part, b"abc").unwrap();
            std::fs::write(dir.path().join(".lock"), b"lock witness").unwrap();
            std::fs::write(dir.path().join("foreign.bin"), b"foreign witness").unwrap();
            let initial = artifact_snapshot(dir.path());
            let witnesses = initial
                .iter()
                .filter(|entry| entry.name == ".lock" || entry.name == "foreign.bin")
                .cloned()
                .collect::<Vec<_>>();
            #[cfg(unix)]
            let directory_identity = {
                use std::os::unix::fs::MetadataExt;
                let metadata = std::fs::metadata(dir.path()).unwrap();
                (metadata.dev(), metadata.ino())
            };
            let transport = FakeTransport {
                responses: RefCell::new(vec![transfer(StatusCode::OK, None, b"abcdef")]),
                offsets: RefCell::new(Vec::new()),
            };
            let pause = Cell::new(false);
            let retries = Cell::new(0);

            let first_failure = download_with_transport_controlled(
                &spec(b"abcdefghi"),
                dir.path(),
                &transport,
                || pause.get(),
                |update| {
                    if matches!(
                        update,
                        ProgressUpdate::Transferring { transferred: 6, .. }
                    ) {
                        pause.set(true);
                    }
                },
                RetryWait {
                    observer: |_| retries.set(retries.get() + 1),
                    sleep: tokio::time::sleep,
                },
                |operation| {
                    let checkpoint = match &operation {
                        ArtifactOperation::Sync { checkpoint, .. }
                        | ArtifactOperation::Observe(checkpoint) => Some(*checkpoint),
                        ArtifactOperation::Write { .. } => None,
                    };
                    perform_artifact_operation(operation)?;
                    if checkpoint == Some(failure_point) {
                        return Err(ArtifactOperationFailure::Other);
                    }
                    Ok(())
                },
            )
            .unwrap_err();

            assert_eq!(first_failure, DownloadFailure::Durability);
            assert_eq!(&*transport.offsets.borrow(), &[Some(3)]);
            assert_eq!(retries.get(), 0);
            assert!(!dir.path().join("model.gguf").exists());
            let crash = artifact_snapshot(dir.path());
            let crash_witnesses = crash
                .iter()
                .filter(|entry| entry.name == ".lock" || entry.name == "foreign.bin")
                .cloned()
                .collect::<Vec<_>>();
            assert_eq!(crash_witnesses, witnesses);
            let longest = crash
                .iter()
                .find(|entry| entry.bytes == b"abcdef")
                .unwrap()
                .clone();
            let (expected_part_length, expected_restart_length) = match phase {
                CrashPhase::BeforeExchange => (Some(3), Some(6)),
                CrashPhase::AfterExchange => (Some(6), Some(3)),
                CrashPhase::PartOnly => (Some(6), None),
            };
            let (audit_directory, _) = crate::safe_file::open_directory(dir.path()).unwrap();
            let audit = match plan::plan_artifact_transfer(
                &audit_directory,
                dir.path(),
                9,
                &hex(Sha256::digest(b"abcdefghi").as_ref()),
                &|| false,
            ) {
                plan::ArtifactPlanOutcome::Ready(plan) => plan,
                plan::ArtifactPlanOutcome::Interrupted => panic!("fresh crash audit interrupted"),
            };
            assert_eq!(audit.state(), plan::ArtifactTransferState::RequestCapable);
            assert_eq!(audit.part_length(), expected_part_length);
            assert_eq!(audit.restart_length(), expected_restart_length);

            let reentry_transport = FakeTransport {
                responses: RefCell::new(vec![transfer(StatusCode::BAD_REQUEST, None, b"")]),
                offsets: RefCell::new(Vec::new()),
            };
            let reentry_progress = RefCell::new(Vec::new());
            let reentry_checkpoints = RefCell::new(Vec::new());
            let reentry_failure = download_with_transport_controlled(
                &spec(b"abcdefghi"),
                dir.path(),
                &reentry_transport,
                || false,
                |update| reentry_progress.borrow_mut().push(update),
                RetryWait {
                    observer: |_| panic!("fatal re-entry response must not retry"),
                    sleep: tokio::time::sleep,
                },
                |operation| {
                    match &operation {
                        ArtifactOperation::Sync { checkpoint, .. }
                        | ArtifactOperation::Observe(checkpoint) => {
                            reentry_checkpoints.borrow_mut().push(*checkpoint);
                        }
                        ArtifactOperation::Write { .. } => {}
                    }
                    perform_artifact_operation(operation)
                },
            )
            .unwrap_err();

            assert_eq!(reentry_failure, DownloadFailure::Remote { retained_bytes: 6 });
            assert_eq!(&*reentry_transport.offsets.borrow(), &[Some(6)]);
            assert_eq!(
                reentry_progress.borrow().first(),
                Some(&ProgressUpdate::Transferring {
                    transferred: 6,
                    total: 9,
                })
            );
            assert_eq!(std::fs::read(&part).unwrap(), b"abcdef");
            assert!(!restart.exists());
            assert!(!dir.path().join("model.gguf").exists());
            let after = artifact_snapshot(dir.path());
            let part_after = after
                .iter()
                .find(|entry| entry.name == "model.gguf.part")
                .unwrap();
            #[cfg(unix)]
            assert_eq!(part_after.inode, longest.inode);
            let after_witnesses = after
                .iter()
                .filter(|entry| entry.name == ".lock" || entry.name == "foreign.bin")
                .cloned()
                .collect::<Vec<_>>();
            assert_eq!(after_witnesses, witnesses);
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                let metadata = std::fs::metadata(dir.path()).unwrap();
                assert_eq!((metadata.dev(), metadata.ino()), directory_identity);
            }
            match phase {
                CrashPhase::BeforeExchange => assert!(reentry_checkpoints
                    .borrow()
                    .contains(&ArtifactCheckpoint::PrefixExchanged)),
                CrashPhase::AfterExchange => {
                    assert!(!reentry_checkpoints
                        .borrow()
                        .contains(&ArtifactCheckpoint::PrefixExchanged));
                    assert!(reentry_checkpoints
                        .borrow()
                        .contains(&ArtifactCheckpoint::RecoveredPartSynced));
                }
                CrashPhase::PartOnly => {
                    assert!(!reentry_checkpoints
                        .borrow()
                        .contains(&ArtifactCheckpoint::PrefixExchanged));
                    assert!(!reentry_checkpoints
                        .borrow()
                        .contains(&ArtifactCheckpoint::BeforeRestartUnlink));
                }
            }
            let (audit_directory, _) = crate::safe_file::open_directory(dir.path()).unwrap();
            let audit = match plan::plan_artifact_transfer(
                &audit_directory,
                dir.path(),
                9,
                &hex(Sha256::digest(b"abcdefghi").as_ref()),
                &|| false,
            ) {
                plan::ArtifactPlanOutcome::Ready(plan) => plan,
                plan::ArtifactPlanOutcome::Interrupted => panic!("final crash audit interrupted"),
            };
            assert_eq!(audit.state(), plan::ArtifactTransferState::RequestCapable);
            assert_eq!(audit.part_length(), Some(6));
            assert_eq!(audit.restart_length(), None);
        }

        for (part_bytes, restart_bytes, expected_part, expected_retained, expected_offset) in [
            (
                b"abcdef".as_slice(),
                Some(b"abc".as_slice()),
                b"abcdef".as_slice(),
                6,
                Some(6),
            ),
            (
                b"abc".as_slice(),
                Some(b"XYZ".as_slice()),
                b"abc".as_slice(),
                3,
                Some(3),
            ),
            (
                b"abc".as_slice(),
                Some(b"".as_slice()),
                b"abc".as_slice(),
                3,
                Some(3),
            ),
            (
                b"".as_slice(),
                Some(b"".as_slice()),
                b"".as_slice(),
                0,
                None,
            ),
            (
                b"abc".as_slice(),
                Some(b"stale".as_slice()),
                b"abc".as_slice(),
                3,
                Some(3),
            ),
            (
                b"abcdef".as_slice(),
                None,
                b"abcdef".as_slice(),
                6,
                Some(6),
            ),
        ] {
            let dir = tempdir().unwrap();
            let part = dir.path().join("model.gguf.part");
            let restart = dir.path().join("model.gguf.part.restart");
            std::fs::write(&part, part_bytes).unwrap();
            if let Some(restart_bytes) = restart_bytes {
                std::fs::write(&restart, restart_bytes).unwrap();
            }
            std::fs::write(dir.path().join(".lock"), b"lock witness").unwrap();
            std::fs::write(dir.path().join("foreign.bin"), b"foreign witness").unwrap();
            let before = artifact_snapshot(dir.path());
            let part_before = before
                .iter()
                .find(|entry| entry.name == "model.gguf.part")
                .unwrap()
                .clone();
            let witnesses_before = before
                .iter()
                .filter(|entry| entry.name == ".lock" || entry.name == "foreign.bin")
                .cloned()
                .collect::<Vec<_>>();
            let checkpoints = RefCell::new(Vec::new());
            let transport = FakeTransport {
                responses: RefCell::new(vec![transfer(StatusCode::BAD_REQUEST, None, b"")]),
                offsets: RefCell::new(Vec::new()),
            };

            let failure = download_with_transport_controlled(
                &spec(b"abcdefghi"),
                dir.path(),
                &transport,
                || false,
                |_| {},
                RetryWait {
                    observer: |_| panic!("fatal re-entry response must not retry"),
                    sleep: tokio::time::sleep,
                },
                |operation| {
                    match &operation {
                        ArtifactOperation::Sync { checkpoint, .. }
                        | ArtifactOperation::Observe(checkpoint) => {
                            checkpoints.borrow_mut().push(*checkpoint);
                        }
                        ArtifactOperation::Write { .. } => {}
                    }
                    perform_artifact_operation(operation)
                },
            )
            .unwrap_err();

            assert_eq!(failure, DownloadFailure::Remote { retained_bytes: expected_retained });
            assert_eq!(&*transport.offsets.borrow(), &[expected_offset]);
            assert_eq!(std::fs::read(&part).unwrap(), expected_part);
            assert!(!restart.exists());
            assert!(!dir.path().join("model.gguf").exists());
            assert!(!checkpoints
                .borrow()
                .contains(&ArtifactCheckpoint::PrefixExchanged));
            let after = artifact_snapshot(dir.path());
            let part_after = after
                .iter()
                .find(|entry| entry.name == "model.gguf.part")
                .unwrap();
            #[cfg(unix)]
            assert_eq!(part_after.inode, part_before.inode);
            let witnesses_after = after
                .iter()
                .filter(|entry| entry.name == ".lock" || entry.name == "foreign.bin")
                .cloned()
                .collect::<Vec<_>>();
            assert_eq!(witnesses_after, witnesses_before);
            if restart_bytes.is_some() {
                assert!(checkpoints
                    .borrow()
                    .contains(&ArtifactCheckpoint::RecoveredPartSynced));
            } else {
                assert!(!checkpoints
                    .borrow()
                    .contains(&ArtifactCheckpoint::BeforeRestartUnlink));
            }
        }

        for (part_bytes, restart_bytes) in [
            (Some(b"".as_slice()), b"abc".as_slice()),
            (Some(b"abcdefghi".as_slice()), b"abc".as_slice()),
            (Some(b"abcdefghij".as_slice()), b"abc".as_slice()),
            (Some(b"abc".as_slice()), b"abcdefghij".as_slice()),
            (None, b"abc".as_slice()),
        ] {
            let dir = tempdir().unwrap();
            if let Some(part_bytes) = part_bytes {
                std::fs::write(dir.path().join("model.gguf.part"), part_bytes).unwrap();
            }
            std::fs::write(
                dir.path().join("model.gguf.part.restart"),
                restart_bytes,
            )
            .unwrap();
            std::fs::write(dir.path().join(".lock"), b"lock witness").unwrap();
            std::fs::write(dir.path().join("foreign.bin"), b"foreign witness").unwrap();
            let before = artifact_snapshot(dir.path());
            let operations = Cell::new(0);
            let transport = FakeTransport {
                responses: RefCell::new(vec![transfer(StatusCode::BAD_REQUEST, None, b"")]),
                offsets: RefCell::new(Vec::new()),
            };

            let failure = download_with_transport_controlled(
                &spec(b"abcdefghi"),
                dir.path(),
                &transport,
                || false,
                |_| {},
                RetryWait {
                    observer: |_| panic!("ambiguous startup state must not retry"),
                    sleep: tokio::time::sleep,
                },
                |operation| {
                    operations.set(operations.get() + 1);
                    perform_artifact_operation(operation)
                },
            )
            .unwrap_err();

            assert_eq!(failure, DownloadFailure::Durability);
            assert!(transport.offsets.borrow().is_empty());
            assert_eq!(operations.get(), 0);
            assert_eq!(artifact_snapshot(dir.path()), before);
        }

        for (part_bytes, restart_bytes, failure_point, expected_offsets) in [
            (
                b"abc".as_slice(),
                Some(b"abcdef".as_slice()),
                ArtifactCheckpoint::BeforePrefixExchange,
                Vec::new(),
            ),
            (
                b"abcdef".as_slice(),
                Some(b"abc".as_slice()),
                ArtifactCheckpoint::RecoveredPartSynced,
                Vec::new(),
            ),
            (
                b"abcdef".as_slice(),
                None,
                ArtifactCheckpoint::StagingSynced,
                vec![Some(6)],
            ),
        ] {
            let dir = tempdir().unwrap();
            std::fs::write(dir.path().join("model.gguf.part"), part_bytes).unwrap();
            if let Some(restart_bytes) = restart_bytes {
                std::fs::write(
                    dir.path().join("model.gguf.part.restart"),
                    restart_bytes,
                )
                .unwrap();
            }
            std::fs::write(dir.path().join(".lock"), b"lock witness").unwrap();
            std::fs::write(dir.path().join("foreign.bin"), b"foreign witness").unwrap();
            let before = artifact_snapshot(dir.path());
            let transport = FakeTransport {
                responses: RefCell::new(vec![transfer(StatusCode::BAD_REQUEST, None, b"")]),
                offsets: RefCell::new(Vec::new()),
            };
            let retries = Cell::new(0);

            let failure = download_with_transport_controlled(
                &spec(b"abcdefghi"),
                dir.path(),
                &transport,
                || false,
                |_| {},
                RetryWait {
                    observer: |_| retries.set(retries.get() + 1),
                    sleep: tokio::time::sleep,
                },
                |operation| {
                    let checkpoint = match &operation {
                        ArtifactOperation::Sync { checkpoint, .. }
                        | ArtifactOperation::Observe(checkpoint) => Some(*checkpoint),
                        ArtifactOperation::Write { .. } => None,
                    };
                    if checkpoint == Some(failure_point) {
                        return Err(ArtifactOperationFailure::Other);
                    }
                    perform_artifact_operation(operation)
                },
            )
            .unwrap_err();

            assert_eq!(failure, DownloadFailure::Durability);
            assert_eq!(&*transport.offsets.borrow(), &expected_offsets);
            assert_eq!(retries.get(), 0);
            assert_eq!(artifact_snapshot(dir.path()), before);
            assert!(!dir.path().join("model.gguf").exists());
        }

        let compare_dir = tempdir().unwrap();
        let compare_part = compare_dir.path().join("model.gguf.part");
        let compare_restart = compare_dir.path().join("model.gguf.part.restart");
        let compare_part_bytes = vec![b'a'; 64 * 1024 + 1];
        let mut compare_restart_bytes = compare_part_bytes.clone();
        compare_restart_bytes.push(b'b');
        let mut compare_expected = compare_restart_bytes.clone();
        compare_expected.push(b'c');
        std::fs::write(&compare_part, &compare_part_bytes).unwrap();
        std::fs::write(&compare_restart, &compare_restart_bytes).unwrap();
        std::fs::write(compare_dir.path().join(".lock"), b"lock witness").unwrap();
        let compare_before = artifact_snapshot(compare_dir.path());
        let compare_checks = Cell::new(0);
        let compare_operations = Cell::new(0);
        let compare_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::BAD_REQUEST, None, b"")]),
            offsets: RefCell::new(Vec::new()),
        };

        let compare_pause = download_with_transport_controlled(
            &spec(&compare_expected),
            compare_dir.path(),
            &compare_transport,
            || {
                let next = compare_checks.get() + 1;
                compare_checks.set(next);
                next >= 6
            },
            |_| {},
            RetryWait {
                observer: |_| panic!("pre-mutation compare Pause must not retry"),
                sleep: tokio::time::sleep,
            },
            |operation| {
                compare_operations.set(compare_operations.get() + 1);
                perform_artifact_operation(operation)
            },
        )
        .unwrap();

        assert_eq!(
            compare_pause,
            DownloadTerminalOutcome::Paused { retained_bytes: 0 }
        );
        assert!(compare_transport.offsets.borrow().is_empty());
        assert_eq!(compare_operations.get(), 0);
        assert_eq!(artifact_snapshot(compare_dir.path()), compare_before);

        #[derive(Clone, Copy)]
        enum CompareMutation {
            Part,
            Restart,
            Directory,
        }

        for mutation in [
            CompareMutation::Part,
            CompareMutation::Restart,
            CompareMutation::Directory,
        ] {
            let root = tempdir().unwrap();
            let model_dir = root.path().join("model");
            let moved_dir = root.path().join("moved-model");
            let part = model_dir.join("model.gguf.part");
            let restart = model_dir.join("model.gguf.part.restart");
            let part_original = model_dir.join("part-original");
            let restart_original = model_dir.join("restart-original");
            std::fs::create_dir(&model_dir).unwrap();
            std::fs::write(&part, b"abc").unwrap();
            std::fs::write(&restart, b"abcdef").unwrap();
            std::fs::write(model_dir.join(".lock"), b"lock witness").unwrap();
            std::fs::write(model_dir.join("foreign.bin"), b"foreign witness").unwrap();
            let checks = Cell::new(0);
            let mutated = Cell::new(false);
            let operations = Cell::new(0);
            let transport = FakeTransport {
                responses: RefCell::new(vec![transfer(StatusCode::BAD_REQUEST, None, b"")]),
                offsets: RefCell::new(Vec::new()),
            };

            let failure = download_with_transport_controlled(
                &spec(b"abcdefghi"),
                &model_dir,
                &transport,
                || {
                    let next = checks.get() + 1;
                    checks.set(next);
                    if next == 6 {
                        match mutation {
                            CompareMutation::Part => {
                                std::fs::rename(&part, &part_original).unwrap();
                                std::fs::write(&part, b"XYZ").unwrap();
                            }
                            CompareMutation::Restart => {
                                std::fs::rename(&restart, &restart_original).unwrap();
                                std::fs::write(&restart, b"UVWXYZ").unwrap();
                            }
                            CompareMutation::Directory => {
                                std::fs::rename(&model_dir, &moved_dir).unwrap();
                                std::fs::create_dir(&model_dir).unwrap();
                                std::fs::write(model_dir.join("replacement-witness"), b"current")
                                    .unwrap();
                            }
                        }
                        mutated.set(true);
                    }
                    false
                },
                |_| {},
                RetryWait {
                    observer: |_| panic!("comparison substitution must not retry"),
                    sleep: tokio::time::sleep,
                },
                |operation| {
                    operations.set(operations.get() + 1);
                    perform_artifact_operation(operation)
                },
            )
            .unwrap_err();

            assert!(mutated.get());
            assert_eq!(failure, DownloadFailure::Durability);
            assert!(transport.offsets.borrow().is_empty());
            assert_eq!(operations.get(), 0);
            match mutation {
                CompareMutation::Part => {
                    assert_eq!(std::fs::read(&part).unwrap(), b"XYZ");
                    assert_eq!(std::fs::read(&part_original).unwrap(), b"abc");
                    assert_eq!(std::fs::read(&restart).unwrap(), b"abcdef");
                    assert_eq!(std::fs::read(model_dir.join(".lock")).unwrap(), b"lock witness");
                    assert_eq!(
                        std::fs::read(model_dir.join("foreign.bin")).unwrap(),
                        b"foreign witness"
                    );
                }
                CompareMutation::Restart => {
                    assert_eq!(std::fs::read(&part).unwrap(), b"abc");
                    assert_eq!(std::fs::read(&restart).unwrap(), b"UVWXYZ");
                    assert_eq!(std::fs::read(&restart_original).unwrap(), b"abcdef");
                    assert_eq!(std::fs::read(model_dir.join(".lock")).unwrap(), b"lock witness");
                    assert_eq!(
                        std::fs::read(model_dir.join("foreign.bin")).unwrap(),
                        b"foreign witness"
                    );
                }
                CompareMutation::Directory => {
                    assert_eq!(
                        std::fs::read(moved_dir.join("model.gguf.part")).unwrap(),
                        b"abc"
                    );
                    assert_eq!(
                        std::fs::read(moved_dir.join("model.gguf.part.restart")).unwrap(),
                        b"abcdef"
                    );
                    assert_eq!(std::fs::read(moved_dir.join(".lock")).unwrap(), b"lock witness");
                    assert_eq!(
                        std::fs::read(moved_dir.join("foreign.bin")).unwrap(),
                        b"foreign witness"
                    );
                    assert_eq!(
                        std::fs::read(model_dir.join("replacement-witness")).unwrap(),
                        b"current"
                    );
                }
            }
            assert!(!model_dir.join("model.gguf").exists());
            assert!(!moved_dir.join("model.gguf").exists());
        }
    }

    #[test]
    fn artifact_discard_cleanup_revalidates_captured_identities_and_unlinks_restart_then_part() {
        fn candidate(path: &std::path::Path) -> ArtifactDiscardFacts {
            let (directory, _) = crate::safe_file::open_directory(path).unwrap();
            plan_artifact_discard(&directory, path).unwrap()
        }

        fn discard_with(
            path: &std::path::Path,
            facts: ArtifactDiscardFacts,
            operation: &mut impl for<'a> FnMut(
                ArtifactOperation<'a>,
            ) -> Result<(), ArtifactOperationFailure>,
        ) -> Result<(), ArtifactDiscardError> {
            let (directory, _) = crate::safe_file::open_directory(path).unwrap();
            artifact::discard_artifact_bytes_controlled((&directory, path), facts, operation)
        }

        let _: fn(
            &std::fs::File,
            &std::path::Path,
            ArtifactDiscardFacts,
        ) -> Result<(), ArtifactDiscardError> = discard_artifact_bytes;

        let restart_trace = [
            ArtifactCheckpoint::BeforeRestartUnlink,
            ArtifactCheckpoint::RestartIdentityMatched,
            ArtifactCheckpoint::RestartUnlinked,
        ];
        let part_trace = [
            ArtifactCheckpoint::BeforeAuthoritativePartUnlink,
            ArtifactCheckpoint::AuthoritativePartIdentityMatched,
            ArtifactCheckpoint::AuthoritativePartUnlinked,
        ];
        let cleanup_trace = [
            ArtifactCheckpoint::ChecksumCleanupDirectorySynced,
            ArtifactCheckpoint::BeforeAuthoritativePartAbsenceProof,
            ArtifactCheckpoint::AuthoritativePartAbsent,
        ];

        for (part_bytes, restart_bytes) in [
            (Some(b"abc".as_slice()), Some(b"abcdef".as_slice())),
            (Some(b"abc".as_slice()), None),
            (None, Some(b"abcdef".as_slice())),
            (None, None),
        ] {
            let dir = tempdir().unwrap();
            if let Some(bytes) = part_bytes {
                std::fs::write(dir.path().join("model.gguf.part"), bytes).unwrap();
            }
            if let Some(bytes) = restart_bytes {
                std::fs::write(dir.path().join("model.gguf.part.restart"), bytes).unwrap();
            }
            for (name, bytes) in [
                (".lock", b"lock witness".as_slice()),
                ("foreign.bin", b"foreign witness".as_slice()),
                ("pending.json", b"catalog witness".as_slice()),
            ] {
                std::fs::write(dir.path().join(name), bytes).unwrap();
            }
            let facts = candidate(dir.path());
            let mut expected = artifact_snapshot(dir.path());
            expected.retain(|entry| {
                entry.name != "model.gguf.part" && entry.name != "model.gguf.part.restart"
            });
            let checkpoints = RefCell::new(Vec::new());

            discard_with(dir.path(), facts, &mut |operation| {
                match &operation {
                    ArtifactOperation::Sync { checkpoint, .. }
                    | ArtifactOperation::Observe(checkpoint) => {
                        checkpoints.borrow_mut().push(*checkpoint);
                    }
                    ArtifactOperation::Write { .. } => panic!("discard must not write bytes"),
                }
                perform_artifact_operation(operation)
            })
            .unwrap();

            assert_eq!(artifact_snapshot(dir.path()), expected);
            let mut expected_trace = Vec::new();
            if restart_bytes.is_some() {
                expected_trace.extend(restart_trace);
            }
            if part_bytes.is_some() {
                expected_trace.extend(part_trace);
            }
            if part_bytes.is_some() || restart_bytes.is_some() {
                expected_trace.extend(cleanup_trace);
            }
            assert_eq!(&*checkpoints.borrow(), &expected_trace);
        }

        for forbidden in ["model.gguf", "model.gguf.invalid"] {
            let dir = tempdir().unwrap();
            std::fs::write(dir.path().join("model.gguf.part"), b"abc").unwrap();
            std::fs::write(dir.path().join(forbidden), b"authority").unwrap();
            let before = artifact_snapshot(dir.path());
            let (directory, _) = crate::safe_file::open_directory(dir.path()).unwrap();
            assert_eq!(
                plan_artifact_discard(&directory, dir.path()).err(),
                Some(ArtifactDiscardError::Changed)
            );
            assert_eq!(artifact_snapshot(dir.path()), before);
        }

        enum PromptMutation {
            ReplacePart,
            ReplaceRestart,
            RemovePart,
            RemoveRestart,
            CreatePart,
            CreateRestart,
            CreateFinal,
            CreateInvalid,
        }
        for mutation in [
            PromptMutation::ReplacePart,
            PromptMutation::ReplaceRestart,
            PromptMutation::RemovePart,
            PromptMutation::RemoveRestart,
            PromptMutation::CreatePart,
            PromptMutation::CreateRestart,
            PromptMutation::CreateFinal,
            PromptMutation::CreateInvalid,
        ] {
            let dir = tempdir().unwrap();
            let part = dir.path().join("model.gguf.part");
            let restart = dir.path().join("model.gguf.part.restart");
            if !matches!(
                mutation,
                PromptMutation::CreatePart
                    | PromptMutation::CreateRestart
                    | PromptMutation::CreateFinal
                    | PromptMutation::CreateInvalid
            ) {
                std::fs::write(&part, b"abc").unwrap();
                std::fs::write(&restart, b"abcdef").unwrap();
            }
            let facts = candidate(dir.path());
            match mutation {
                PromptMutation::ReplacePart => {
                    std::fs::rename(&part, dir.path().join("captured-part")).unwrap();
                    std::fs::write(&part, b"XYZ").unwrap();
                }
                PromptMutation::ReplaceRestart => {
                    std::fs::rename(&restart, dir.path().join("captured-restart")).unwrap();
                    std::fs::write(&restart, b"UVWXYZ").unwrap();
                }
                PromptMutation::RemovePart => std::fs::remove_file(&part).unwrap(),
                PromptMutation::RemoveRestart => std::fs::remove_file(&restart).unwrap(),
                PromptMutation::CreatePart => std::fs::write(&part, b"new").unwrap(),
                PromptMutation::CreateRestart => {
                    std::fs::write(&restart, b"new restart").unwrap()
                }
                PromptMutation::CreateFinal => {
                    std::fs::write(dir.path().join("model.gguf"), b"installed").unwrap()
                }
                PromptMutation::CreateInvalid => {
                    std::fs::write(dir.path().join("model.gguf.invalid"), b"repair").unwrap()
                }
            }
            let expected = artifact_snapshot(dir.path());
            let operations = Cell::new(0);

            assert_eq!(
                discard_with(dir.path(), facts, &mut |_| {
                    operations.set(operations.get() + 1);
                    Ok(())
                }),
                Err(ArtifactDiscardError::Changed)
            );
            assert_eq!(operations.get(), 0);
            assert_eq!(artifact_snapshot(dir.path()), expected);
        }

        let identity_root = tempdir().unwrap();
        let identity_dir = identity_root.path().join("model");
        let moved_identity_dir = identity_root.path().join("captured-model");
        std::fs::create_dir(&identity_dir).unwrap();
        let identity_facts = candidate(&identity_dir);
        std::fs::rename(&identity_dir, &moved_identity_dir).unwrap();
        std::fs::create_dir(&identity_dir).unwrap();
        std::fs::write(identity_dir.join("replacement-witness"), b"current").unwrap();
        std::fs::write(moved_identity_dir.join("captured-witness"), b"captured").unwrap();
        let identity_operations = Cell::new(0);
        assert_eq!(
            discard_with(&identity_dir, identity_facts, &mut |_| {
                identity_operations.set(identity_operations.get() + 1);
                Ok(())
            }),
            Err(ArtifactDiscardError::Changed)
        );
        assert_eq!(identity_operations.get(), 0);
        assert_eq!(
            std::fs::read(identity_dir.join("replacement-witness")).unwrap(),
            b"current"
        );
        assert_eq!(
            std::fs::read(moved_identity_dir.join("captured-witness")).unwrap(),
            b"captured"
        );

        #[derive(Clone, Copy)]
        enum HookMutation {
            Part,
            Restart,
        }
        for mutation in [HookMutation::Part, HookMutation::Restart] {
            let hook_dir = tempdir().unwrap();
            let hook_part = hook_dir.path().join("model.gguf.part");
            let hook_restart = hook_dir.path().join("model.gguf.part.restart");
            std::fs::write(&hook_part, b"abc").unwrap();
            std::fs::write(&hook_restart, b"abcdef").unwrap();
            let hook_facts = candidate(hook_dir.path());
            let hook_trace = RefCell::new(Vec::new());
            assert_eq!(
                discard_with(hook_dir.path(), hook_facts, &mut |operation| {
                    let checkpoint = match &operation {
                        ArtifactOperation::Sync { checkpoint, .. }
                        | ArtifactOperation::Observe(checkpoint) => *checkpoint,
                        ArtifactOperation::Write { .. } => panic!("discard must not write bytes"),
                    };
                    hook_trace.borrow_mut().push(checkpoint);
                    if checkpoint == ArtifactCheckpoint::RestartIdentityMatched {
                        match mutation {
                            HookMutation::Part => {
                                std::fs::rename(
                                    &hook_part,
                                    hook_dir.path().join("captured-part"),
                                )
                                .unwrap();
                                std::fs::write(&hook_part, b"XYZ").unwrap();
                            }
                            HookMutation::Restart => {
                                std::fs::rename(
                                    &hook_restart,
                                    hook_dir.path().join("captured-restart"),
                                )
                                .unwrap();
                                std::fs::write(&hook_restart, b"UVWXYZ").unwrap();
                            }
                        }
                    }
                    perform_artifact_operation(operation)
                }),
                Err(ArtifactDiscardError::Changed)
            );
            assert_eq!(
                &*hook_trace.borrow(),
                &[
                    ArtifactCheckpoint::BeforeRestartUnlink,
                    ArtifactCheckpoint::RestartIdentityMatched,
                ]
            );
            match mutation {
                HookMutation::Part => {
                    assert_eq!(std::fs::read(&hook_part).unwrap(), b"XYZ");
                    assert_eq!(
                        std::fs::read(hook_dir.path().join("captured-part")).unwrap(),
                        b"abc"
                    );
                    assert_eq!(std::fs::read(&hook_restart).unwrap(), b"abcdef");
                }
                HookMutation::Restart => {
                    assert_eq!(std::fs::read(&hook_part).unwrap(), b"abc");
                    assert_eq!(std::fs::read(&hook_restart).unwrap(), b"UVWXYZ");
                    assert_eq!(
                        std::fs::read(hook_dir.path().join("captured-restart")).unwrap(),
                        b"abcdef"
                    );
                }
            }
        }

        for swap_directory in [false, true] {
            let root = tempdir().unwrap();
            let model_dir = root.path().join("model");
            let moved_dir = root.path().join("captured-model");
            std::fs::create_dir(&model_dir).unwrap();
            let part = model_dir.join("model.gguf.part");
            let captured_part = model_dir.join("captured-part");
            std::fs::write(&part, b"abc").unwrap();
            let facts = candidate(&model_dir);
            let trace = RefCell::new(Vec::new());
            assert_eq!(
                discard_with(&model_dir, facts, &mut |operation| {
                    let checkpoint = match &operation {
                        ArtifactOperation::Sync { checkpoint, .. }
                        | ArtifactOperation::Observe(checkpoint) => *checkpoint,
                        ArtifactOperation::Write { .. } => panic!("discard must not write bytes"),
                    };
                    trace.borrow_mut().push(checkpoint);
                    if checkpoint == ArtifactCheckpoint::AuthoritativePartIdentityMatched {
                        if swap_directory {
                            std::fs::rename(&model_dir, &moved_dir).unwrap();
                            std::fs::create_dir(&model_dir).unwrap();
                            std::fs::write(model_dir.join("replacement-witness"), b"current")
                                .unwrap();
                        } else {
                            std::fs::rename(&part, &captured_part).unwrap();
                            std::fs::write(&part, b"XYZ").unwrap();
                        }
                    }
                    perform_artifact_operation(operation)
                }),
                Err(ArtifactDiscardError::Changed)
            );
            assert_eq!(
                &*trace.borrow(),
                &[
                    ArtifactCheckpoint::BeforeAuthoritativePartUnlink,
                    ArtifactCheckpoint::AuthoritativePartIdentityMatched,
                ]
            );
            if swap_directory {
                assert_eq!(
                    std::fs::read(moved_dir.join("model.gguf.part")).unwrap(),
                    b"abc"
                );
                assert_eq!(
                    std::fs::read(model_dir.join("replacement-witness")).unwrap(),
                    b"current"
                );
            } else {
                assert_eq!(std::fs::read(&part).unwrap(), b"XYZ");
                assert_eq!(std::fs::read(&captured_part).unwrap(), b"abc");
            }
        }

        for failing_checkpoint in [
            ArtifactCheckpoint::BeforeRestartUnlink,
            ArtifactCheckpoint::ChecksumCleanupDirectorySynced,
        ] {
            let dir = tempdir().unwrap();
            let part = dir.path().join("model.gguf.part");
            let restart = dir.path().join("model.gguf.part.restart");
            std::fs::write(&part, b"abc").unwrap();
            std::fs::write(&restart, b"abcdef").unwrap();
            let facts = candidate(dir.path());
            assert_eq!(
                discard_with(dir.path(), facts, &mut |operation| {
                    let checkpoint = match &operation {
                        ArtifactOperation::Sync { checkpoint, .. }
                        | ArtifactOperation::Observe(checkpoint) => *checkpoint,
                        ArtifactOperation::Write { .. } => panic!("discard must not write bytes"),
                    };
                    if checkpoint == failing_checkpoint {
                        Err(ArtifactOperationFailure::Other)
                    } else {
                        perform_artifact_operation(operation)
                    }
                }),
                Err(ArtifactDiscardError::Durability)
            );
            let expected_present =
                failing_checkpoint == ArtifactCheckpoint::BeforeRestartUnlink;
            assert_eq!(part.exists(), expected_present);
            assert_eq!(restart.exists(), expected_present);
        }

        for reappearing_name in [
            "model.gguf.part.restart",
            "model.gguf.part",
            "model.gguf",
            "model.gguf.invalid",
        ] {
            let dir = tempdir().unwrap();
            let part = dir.path().join("model.gguf.part");
            let restart = dir.path().join("model.gguf.part.restart");
            let reappearing = dir.path().join(reappearing_name);
            std::fs::write(&part, b"abc").unwrap();
            std::fs::write(&restart, b"abcdef").unwrap();
            let facts = candidate(dir.path());
            assert_eq!(
                discard_with(dir.path(), facts, &mut |operation| {
                    if matches!(
                        &operation,
                        ArtifactOperation::Observe(ArtifactCheckpoint::AuthoritativePartAbsent)
                    ) {
                        std::fs::write(&reappearing, b"reappeared").unwrap();
                    }
                    perform_artifact_operation(operation)
                }),
                Err(ArtifactDiscardError::Durability)
            );
            assert_eq!(std::fs::read(&reappearing).unwrap(), b"reappeared");
            if reappearing_name != "model.gguf.part" {
                assert!(!part.exists());
            }
            if reappearing_name != "model.gguf.part.restart" {
                assert!(!restart.exists());
            }
        }

        let swap_root = tempdir().unwrap();
        let swap_dir = swap_root.path().join("model");
        let moved_dir = swap_root.path().join("moved-model");
        std::fs::create_dir(&swap_dir).unwrap();
        std::fs::write(swap_dir.join("model.gguf.part"), b"abc").unwrap();
        std::fs::write(swap_dir.join("model.gguf.part.restart"), b"abcdef").unwrap();
        std::fs::write(swap_dir.join(".lock"), b"original lock").unwrap();
        let swap_facts = candidate(&swap_dir);
        assert_eq!(discard_with(&swap_dir, swap_facts, &mut |operation| {
            if matches!(
                &operation,
                ArtifactOperation::Observe(ArtifactCheckpoint::AuthoritativePartAbsent)
            ) {
                std::fs::rename(&swap_dir, &moved_dir).unwrap();
                std::fs::create_dir(&swap_dir).unwrap();
                std::fs::write(swap_dir.join("replacement-witness"), b"current").unwrap();
            }
            perform_artifact_operation(operation)
        }), Err(ArtifactDiscardError::Durability));
        assert!(!moved_dir.join("model.gguf.part").exists());
        assert!(!moved_dir.join("model.gguf.part.restart").exists());
        assert_eq!(std::fs::read(moved_dir.join(".lock")).unwrap(), b"original lock");
        assert_eq!(
            std::fs::read(swap_dir.join("replacement-witness")).unwrap(),
            b"current"
        );
    }

    #[test]
    fn artifact_discard_cleanup_syncs_after_bytes_and_never_touches_final_invalid_lock_or_foreign_files(
    ) {
        fn candidate(path: &std::path::Path) -> ArtifactDiscardFacts {
            let (directory, _) = crate::safe_file::open_directory(path).unwrap();
            plan_artifact_discard(&directory, path).unwrap()
        }

        fn discard_with(
            path: &std::path::Path,
            facts: ArtifactDiscardFacts,
            operation: &mut impl for<'a> FnMut(
                ArtifactOperation<'a>,
            ) -> Result<(), ArtifactOperationFailure>,
        ) -> Result<(), ArtifactDiscardError> {
            let (directory, _) = crate::safe_file::open_directory(path).unwrap();
            artifact::discard_artifact_bytes_controlled((&directory, path), facts, operation)
        }

        fn witnesses(path: &std::path::Path) -> Vec<ArtifactSnapshotEntry> {
            artifact_snapshot(path)
                .into_iter()
                .filter(|entry| entry.name == ".lock" || entry.name == "foreign.bin")
                .collect()
        }

        for fail_sync in [false, true] {
            let dir = tempdir().unwrap();
            let part = dir.path().join("model.gguf.part");
            let restart = dir.path().join("model.gguf.part.restart");
            std::fs::write(&part, b"abc").unwrap();
            std::fs::write(&restart, b"abcdef").unwrap();
            std::fs::write(dir.path().join(".lock"), b"lock witness").unwrap();
            std::fs::write(dir.path().join("foreign.bin"), b"foreign witness").unwrap();
            let expected_witnesses = witnesses(dir.path());
            let facts = candidate(dir.path());
            let syncs = Cell::new(0);

            let result = discard_with(dir.path(), facts, &mut |operation| match operation {
                ArtifactOperation::Sync {
                    checkpoint: ArtifactCheckpoint::ChecksumCleanupDirectorySynced,
                    ..
                } => {
                    syncs.set(syncs.get() + 1);
                    assert!(!part.exists());
                    assert!(!restart.exists());
                    assert_eq!(witnesses(dir.path()), expected_witnesses);
                    if fail_sync {
                        Err(ArtifactOperationFailure::Other)
                    } else {
                        perform_artifact_operation(operation)
                    }
                }
                ArtifactOperation::Sync { .. } => panic!("discard emitted an extra sync"),
                ArtifactOperation::Write { .. } => panic!("discard must not write bytes"),
                ArtifactOperation::Observe(_) => perform_artifact_operation(operation),
            });

            assert_eq!(syncs.get(), 1);
            assert!(!part.exists());
            assert!(!restart.exists());
            assert_eq!(witnesses(dir.path()), expected_witnesses);
            assert_eq!(
                result,
                if fail_sync {
                    Err(ArtifactDiscardError::Durability)
                } else {
                    Ok(())
                }
            );
        }

        for forbidden in ["model.gguf", "model.gguf.invalid"] {
            let planned = tempdir().unwrap();
            std::fs::write(planned.path().join("model.gguf.part"), b"abc").unwrap();
            std::fs::write(planned.path().join("model.gguf.part.restart"), b"abcdef").unwrap();
            std::fs::write(planned.path().join(forbidden), b"authority").unwrap();
            let planned_before = artifact_snapshot(planned.path());
            let (directory, _) = crate::safe_file::open_directory(planned.path()).unwrap();
            assert_eq!(
                plan_artifact_discard(&directory, planned.path()).err(),
                Some(ArtifactDiscardError::Changed)
            );
            assert_eq!(artifact_snapshot(planned.path()), planned_before);

            let changed = tempdir().unwrap();
            std::fs::write(changed.path().join("model.gguf.part"), b"abc").unwrap();
            std::fs::write(
                changed.path().join("model.gguf.part.restart"),
                b"abcdef",
            )
            .unwrap();
            let facts = candidate(changed.path());
            std::fs::write(changed.path().join(forbidden), b"authority").unwrap();
            let changed_before = artifact_snapshot(changed.path());
            let operations = Cell::new(0);
            assert_eq!(
                discard_with(changed.path(), facts, &mut |_| {
                    operations.set(operations.get() + 1);
                    Ok(())
                }),
                Err(ArtifactDiscardError::Changed)
            );
            assert_eq!(operations.get(), 0);
            assert_eq!(artifact_snapshot(changed.path()), changed_before);
        }

        let empty = tempdir().unwrap();
        let facts = candidate(empty.path());
        let operations = Cell::new(0);
        assert_eq!(
            discard_with(empty.path(), facts, &mut |_| {
                operations.set(operations.get() + 1);
                Ok(())
            }),
            Ok(())
        );
        assert_eq!(operations.get(), 0);
        assert!(artifact_snapshot(empty.path()).is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn artifact_discard_cleanup_refuses_stale_symlink_hard_link_or_substitution_without_deletion()
    {
        use std::os::unix::fs::symlink;

        fn candidate(path: &std::path::Path) -> ArtifactDiscardFacts {
            let (directory, _) = crate::safe_file::open_directory(path).unwrap();
            plan_artifact_discard(&directory, path).unwrap()
        }

        fn discard_with(
            path: &std::path::Path,
            facts: ArtifactDiscardFacts,
            operations: &Cell<usize>,
        ) -> Result<(), ArtifactDiscardError> {
            let (directory, _) = crate::safe_file::open_directory(path).unwrap();
            artifact::discard_artifact_bytes_controlled(
                (&directory, path),
                facts,
                &mut |_| {
                    operations.set(operations.get() + 1);
                    Ok(())
                },
            )
        }

        fn outside_snapshot(path: &std::path::Path) -> (Vec<u8>, u64, u64, u64) {
            use std::os::unix::fs::MetadataExt;

            let metadata = std::fs::metadata(path).unwrap();
            (
                std::fs::read(path).unwrap(),
                metadata.dev(),
                metadata.ino(),
                metadata.nlink(),
            )
        }

        let entries = [
            ("model.gguf.part", b"abc".as_slice(), b"XYZ".as_slice()),
            (
                "model.gguf.part.restart",
                b"abcdef".as_slice(),
                b"UVWXYZ".as_slice(),
            ),
        ];

        for (name, original, _) in entries {
            for hard_link in [false, true] {
                let dir = tempdir().unwrap();
                let outside_dir = tempdir().unwrap();
                let target = dir.path().join(name);
                let outside = outside_dir.path().join("outside.bin");
                std::fs::write(&outside, original).unwrap();
                if hard_link {
                    std::fs::hard_link(&outside, &target).unwrap();
                } else {
                    symlink(&outside, &target).unwrap();
                }
                let before = artifact_snapshot(dir.path());
                let outside_before = outside_snapshot(&outside);
                let (directory, _) = crate::safe_file::open_directory(dir.path()).unwrap();

                assert_eq!(
                    plan_artifact_discard(&directory, dir.path()).err(),
                    Some(ArtifactDiscardError::Changed)
                );
                assert_eq!(artifact_snapshot(dir.path()), before);
                assert_eq!(outside_snapshot(&outside), outside_before);
            }
        }

        #[derive(Clone, Copy)]
        enum Replacement {
            Regular,
            Symlink,
            HardLink,
        }
        for (name, original, replacement) in entries {
            for kind in [
                Replacement::Regular,
                Replacement::Symlink,
                Replacement::HardLink,
            ] {
                let dir = tempdir().unwrap();
                let outside_dir = tempdir().unwrap();
                let target = dir.path().join(name);
                let captured = dir.path().join("captured-original");
                let outside = outside_dir.path().join("outside.bin");
                std::fs::write(&target, original).unwrap();
                let facts = candidate(dir.path());
                std::fs::rename(&target, &captured).unwrap();
                std::fs::write(&outside, replacement).unwrap();
                match kind {
                    Replacement::Regular => std::fs::write(&target, replacement).unwrap(),
                    Replacement::Symlink => symlink(&outside, &target).unwrap(),
                    Replacement::HardLink => std::fs::hard_link(&outside, &target).unwrap(),
                }
                let before = artifact_snapshot(dir.path());
                let outside_before = outside_snapshot(&outside);
                let operations = Cell::new(0);

                assert_eq!(
                    discard_with(dir.path(), facts, &operations),
                    Err(ArtifactDiscardError::Changed)
                );
                assert_eq!(operations.get(), 0);
                assert_eq!(artifact_snapshot(dir.path()), before);
                assert_eq!(outside_snapshot(&outside), outside_before);
            }
        }
    }

    #[test]
    fn pause_before_request_returns_without_transport_or_retry() {
        let dir = tempdir().unwrap();
        let transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, b"abcdef")]),
            offsets: RefCell::new(Vec::new()),
        };
        let delay_calls = Cell::new(0);

        let outcome = download_with_transport_controlled(
            &spec(b"abcdef"),
            dir.path(),
            &transport,
            || true,
            |_| {},
            RetryWait { observer: |_| {
                delay_calls.set(delay_calls.get() + 1);
            }, sleep: tokio::time::sleep },
            perform_artifact_operation,
        )
        .unwrap();

        assert!(matches!(
            outcome,
            DownloadTerminalOutcome::Paused { retained_bytes: 0 }
        ));
        assert!(transport.offsets.borrow().is_empty());
        assert_eq!(delay_calls.get(), 0);
        assert!(std::fs::read_dir(dir.path()).unwrap().next().is_none());
    }

    #[test]
    fn pause_during_request_wait_stops_before_a_later_request() {
        use std::sync::atomic::Ordering;
        use std::sync::{mpsc, Arc};

        let gate = Arc::new(RequestGate {
            pause: std::sync::atomic::AtomicBool::new(false),
            release: std::sync::atomic::AtomicBool::new(false),
            requests: std::sync::atomic::AtomicUsize::new(0),
            polls: std::sync::atomic::AtomicUsize::new(0),
            offsets: std::sync::Mutex::new(Vec::new()),
            waker: std::sync::Mutex::new(None),
        });
        let worker_gate = Arc::clone(&gate);
        let (sender, receiver) = mpsc::channel();
        let (started_sender, started_receiver) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            let dir = tempdir().unwrap();
            let transport = PausingRequestTransport {
                gate: Arc::clone(&worker_gate),
                started: std::sync::Mutex::new(Some(started_sender)),
            };
            let retry_notifications = Cell::new(0);
            let outcome = download_with_transport_controlled(
                &spec(b"abcdef"),
                dir.path(),
                &transport,
                || worker_gate.pause.load(Ordering::SeqCst),
                |_| {},
                RetryWait { observer: |_| retry_notifications.set(retry_notifications.get() + 1), sleep: tokio::time::sleep },
                perform_artifact_operation,
            );
            let files = std::fs::read_dir(dir.path()).unwrap().count();
            let _ = sender.send((outcome, retry_notifications.get(), files));
        });

        if let Err(error) = started_receiver.recv_timeout(std::time::Duration::from_secs(1)) {
            gate.release.store(true, Ordering::SeqCst);
            if let Some(waker) = gate.waker.lock().unwrap().take() {
                waker.wake();
            }
            let _ = receiver.recv_timeout(std::time::Duration::from_secs(1));
            worker.join().unwrap();
            panic!("request future did not reach its first poll: {error:?}");
        }
        let first = receiver.recv_timeout(std::time::Duration::from_millis(250));
        let returned_without_release = first.is_ok();
        let result = match first {
            Ok(result) => result,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                gate.release.store(true, Ordering::SeqCst);
                if let Some(waker) = gate.waker.lock().unwrap().take() {
                    waker.wake();
                }
                receiver
                    .recv_timeout(std::time::Duration::from_secs(1))
                    .expect("released request worker must finish")
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                worker.join().unwrap();
                panic!("request worker disconnected")
            }
        };
        worker.join().unwrap();

        assert!(returned_without_release, "request wait ignored sticky Pause");
        assert!(matches!(
            result.0,
            Ok(DownloadTerminalOutcome::Paused { retained_bytes: 0 })
        ));
        assert_eq!(result.1, 0);
        assert_eq!(result.2, 0);
        assert_eq!(gate.requests.load(Ordering::SeqCst), 1);
        assert!(gate.polls.load(Ordering::SeqCst) >= 1);
        assert_eq!(&*gate.offsets.lock().unwrap(), &[None]);
    }

    #[test]
    fn pause_during_redirect_wait_stops_before_following_location() {
        let dir = tempdir().unwrap();
        let pause = Cell::new(false);
        let transport = RedirectingTransport {
            pause: &pause,
            requests: RefCell::new(Vec::new()),
        };
        let retry_notifications = Cell::new(0);

        let outcome = download_with_transport_controlled(
            &spec(b"abcdef"),
            dir.path(),
            &transport,
            || pause.get(),
            |_| {},
            RetryWait { observer: |_| retry_notifications.set(retry_notifications.get() + 1), sleep: tokio::time::sleep },
            perform_artifact_operation,
        )
        .unwrap();

        assert!(matches!(
            outcome,
            DownloadTerminalOutcome::Paused { retained_bytes: 0 }
        ));
        let requests = transport.requests.borrow();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].0.host_str(), Some("huggingface.co"));
        assert_eq!(requests[0].1, None);
        assert!(requests[0].2);
        assert!(!requests.iter().any(|(url, _, _)| {
            url.host_str() == Some("cdn.example")
        }));
        assert_eq!(retry_notifications.get(), 0);
        assert!(std::fs::read_dir(dir.path()).unwrap().next().is_none());
    }

    #[test]
    fn pause_during_body_wait_accepts_no_later_chunk() {
        use std::sync::atomic::Ordering;
        use std::sync::{mpsc, Arc};

        let gate = Arc::new(BodyGate {
            pause: std::sync::atomic::AtomicBool::new(false),
            release: std::sync::atomic::AtomicBool::new(false),
            polls: std::sync::atomic::AtomicUsize::new(0),
            waker: std::sync::Mutex::new(None),
        });
        let worker_gate = Arc::clone(&gate);
        let (sender, receiver) = mpsc::channel();
        let (started_sender, started_receiver) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            let dir = tempdir().unwrap();
            let future_gate = Arc::clone(&worker_gate);
            let mut started = Some(started_sender);
            let body = std::future::poll_fn(move |context| {
                future_gate.polls.fetch_add(1, Ordering::SeqCst);
                future_gate.pause.store(true, Ordering::SeqCst);
                if let Some(started) = started.take() {
                    let _ = started.send(());
                }
                if future_gate.release.load(Ordering::SeqCst) {
                    std::task::Poll::Ready(Ok(Some(b"abcdef".to_vec())))
                } else {
                    *future_gate.waker.lock().unwrap() = Some(context.waker().clone());
                    std::task::Poll::Pending
                }
            });
            let transport = FakeTransport {
                responses: RefCell::new(vec![Transfer::test_future(
                    StatusCode::OK,
                    None,
                    body,
                )]),
                offsets: RefCell::new(Vec::new()),
            };
            let retry_notifications = Cell::new(0);
            let outcome = download_with_transport_controlled(
                &spec(b"abcdef"),
                dir.path(),
                &transport,
                || worker_gate.pause.load(Ordering::SeqCst),
                |_| {},
                RetryWait { observer: |_| retry_notifications.set(retry_notifications.get() + 1), sleep: tokio::time::sleep },
                perform_artifact_operation,
            );
            let part_path = dir.path().join("model.gguf.part");
            let part_exists = part_path.exists();
            let part = std::fs::read(&part_path).unwrap_or_default();
            let final_exists = dir.path().join("model.gguf").exists();
            let restart_exists = dir.path().join("model.gguf.part.restart").exists();
            let _ = sender.send((
                outcome,
                retry_notifications.get(),
                transport.offsets.into_inner(),
                part_exists,
                part,
                final_exists,
                restart_exists,
            ));
        });

        if let Err(error) = started_receiver.recv_timeout(std::time::Duration::from_secs(1)) {
            gate.release.store(true, Ordering::SeqCst);
            if let Some(waker) = gate.waker.lock().unwrap().take() {
                waker.wake();
            }
            let _ = receiver.recv_timeout(std::time::Duration::from_secs(1));
            worker.join().unwrap();
            panic!("body future did not reach its first poll: {error:?}");
        }
        let first = receiver.recv_timeout(std::time::Duration::from_millis(250));
        let returned_without_release = first.is_ok();
        let result = match first {
            Ok(result) => result,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                gate.release.store(true, Ordering::SeqCst);
                if let Some(waker) = gate.waker.lock().unwrap().take() {
                    waker.wake();
                }
                receiver
                    .recv_timeout(std::time::Duration::from_secs(1))
                    .expect("released body worker must finish")
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                worker.join().unwrap();
                panic!("body worker disconnected")
            }
        };
        worker.join().unwrap();

        assert!(returned_without_release, "body wait ignored sticky Pause");
        assert!(matches!(
            result.0,
            Ok(DownloadTerminalOutcome::Paused { retained_bytes: 0 })
        ));
        assert_eq!(result.1, 0);
        assert_eq!(result.2, [None]);
        assert!(result.3);
        assert!(result.4.is_empty());
        assert!(!result.5);
        assert!(!result.6);
        assert!(gate.polls.load(Ordering::SeqCst) >= 1);
    }

    #[test]
    fn pause_after_chunk_writes_only_the_winning_chunk() {
        use std::sync::atomic::Ordering;
        use std::sync::Arc;

        let dir = tempdir().unwrap();
        let pause = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let losing_polls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let losing_poll_count = Arc::clone(&losing_polls);
        let chunks = vec![
            body_future(std::future::ready(Ok(Some(b"abc".to_vec())))),
            body_future(std::future::poll_fn(move |_| {
                losing_poll_count.fetch_add(1, Ordering::SeqCst);
                std::task::Poll::Ready(Ok(Some(b"def".to_vec())))
            })),
        ];
        let transport = FakeTransport {
            responses: RefCell::new(vec![Transfer::test_futures(
                StatusCode::OK,
                None,
                chunks,
            )]),
            offsets: RefCell::new(Vec::new()),
        };
        let retry_notifications = Cell::new(0);
        let progress = RefCell::new(Vec::new());

        let outcome = download_with_transport_controlled(
            &spec(b"abcdef"),
            dir.path(),
            &transport,
            || pause.load(Ordering::SeqCst),
            |update| {
                if update
                    == (ProgressUpdate::Transferring {
                        transferred: 3,
                        total: 6,
                    })
                {
                    pause.store(true, Ordering::SeqCst);
                }
                progress.borrow_mut().push(update);
            },
            RetryWait { observer: |_| retry_notifications.set(retry_notifications.get() + 1), sleep: tokio::time::sleep },
            perform_artifact_operation,
        )
        .unwrap();

        assert!(matches!(
            outcome,
            DownloadTerminalOutcome::Paused { retained_bytes: 3 }
        ));
        assert_eq!(&*transport.offsets.borrow(), &[None]);
        assert_eq!(losing_polls.load(Ordering::SeqCst), 0);
        assert_eq!(retry_notifications.get(), 0);
        assert_eq!(
            std::fs::read(dir.path().join("model.gguf.part")).unwrap(),
            b"abc"
        );
        assert!(!dir.path().join("model.gguf").exists());
        assert!(!dir.path().join("model.gguf.part.restart").exists());
        assert_eq!(
            &*progress.borrow(),
            &[
                ProgressUpdate::Transferring {
                    transferred: 0,
                    total: 6,
                },
                ProgressUpdate::Transferring {
                    transferred: 3,
                    total: 6,
                },
            ]
        );
    }

    #[test]
    fn pause_during_retry_backoff_prevents_the_next_attempt() {
        use std::sync::atomic::Ordering;
        use std::sync::{mpsc, Arc};

        let (started_sender, started_receiver) = mpsc::channel();
        let gate = Arc::new(RetryDelayGate {
            pause: std::sync::atomic::AtomicBool::new(false),
            release: std::sync::atomic::AtomicBool::new(false),
            polls: std::sync::atomic::AtomicUsize::new(0),
            delays: std::sync::Mutex::new(Vec::new()),
            started: std::sync::Mutex::new(Some(started_sender)),
            waker: std::sync::Mutex::new(None),
        });
        let worker_gate = Arc::clone(&gate);
        let (sender, receiver) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            let dir = tempdir().unwrap();
            let transport = FailingTransport {
                remaining_failures: Cell::new(1),
                retryable: true,
                attempts: Cell::new(0),
                offsets: RefCell::new(Vec::new()),
                bytes: b"abcdef".to_vec(),
            };
            let retry_notifications = RefCell::new(Vec::new());
            let should_pause_gate = Arc::clone(&worker_gate);
            let sleep_gate = Arc::clone(&worker_gate);
            let outcome = download_with_transport_controlled(
                &spec(b"abcdef"),
                dir.path(),
                &transport,
                || should_pause_gate.pause.load(Ordering::SeqCst),
                |_| {},
                RetryWait { observer: |delay| retry_notifications.borrow_mut().push(delay), sleep: move |delay| {
                    sleep_gate.delays.lock().unwrap().push(delay);
                    let future_gate = Arc::clone(&sleep_gate);
                    std::future::poll_fn(move |context| {
                        future_gate.polls.fetch_add(1, Ordering::SeqCst);
                        future_gate.pause.store(true, Ordering::SeqCst);
                        if let Some(started) = future_gate.started.lock().unwrap().take() {
                            let _ = started.send(());
                        }
                        if future_gate.release.load(Ordering::SeqCst) {
                            std::task::Poll::Ready(())
                        } else {
                            *future_gate.waker.lock().unwrap() = Some(context.waker().clone());
                            std::task::Poll::Pending
                        }
                    })
                } },
                perform_artifact_operation,
            );
            let _ = sender.send((
                outcome,
                transport.attempts.get(),
                transport.offsets.into_inner(),
                retry_notifications.into_inner(),
                std::fs::read_dir(dir.path()).unwrap().count(),
            ));
        });

        if let Err(error) = started_receiver.recv_timeout(std::time::Duration::from_secs(1)) {
            gate.release.store(true, Ordering::SeqCst);
            if let Some(waker) = gate.waker.lock().unwrap().take() {
                waker.wake();
            }
            let _ = receiver.recv_timeout(std::time::Duration::from_secs(1));
            worker.join().unwrap();
            panic!("retry delay did not reach its first poll: {error:?}");
        }
        let first = receiver.recv_timeout(std::time::Duration::from_millis(250));
        let returned_without_release = first.is_ok();
        let result = match first {
            Ok(result) => result,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                gate.release.store(true, Ordering::SeqCst);
                if let Some(waker) = gate.waker.lock().unwrap().take() {
                    waker.wake();
                }
                receiver
                    .recv_timeout(std::time::Duration::from_secs(1))
                    .expect("released retry worker must finish")
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                worker.join().unwrap();
                panic!("retry worker disconnected")
            }
        };
        worker.join().unwrap();

        assert!(returned_without_release, "retry backoff ignored sticky Pause");
        assert!(matches!(
            result.0,
            Ok(DownloadTerminalOutcome::Paused { retained_bytes: 0 })
        ));
        assert_eq!(result.1, 1);
        assert_eq!(result.2, [None]);
        assert_eq!(result.3.len(), 1);
        assert_eq!(&*gate.delays.lock().unwrap(), &result.3);
        assert!(result.3[0] > std::time::Duration::ZERO);
        assert_eq!(result.4, 0);
        assert!(gate.polls.load(Ordering::SeqCst) >= 1);
    }

    #[test]
    fn request_and_body_failures_keep_the_incumbent_four_attempt_bound_when_not_paused() {
        let request_dir = tempdir().unwrap();
        let request_transport = FailingTransport {
            remaining_failures: Cell::new(usize::MAX),
            retryable: true,
            attempts: Cell::new(0),
            offsets: RefCell::new(Vec::new()),
            bytes: b"abcdef".to_vec(),
        };
        let request_notifications = RefCell::new(Vec::new());
        let request_sleeps = RefCell::new(Vec::new());

        let request_error = download_with_transport_controlled(
            &spec(b"abcdef"),
            request_dir.path(),
            &request_transport,
            || false,
            |_| {},
            RetryWait { observer: |delay| request_notifications.borrow_mut().push(delay), sleep: |delay| {
                request_sleeps.borrow_mut().push(delay);
                std::future::ready(())
            } },
            perform_artifact_operation,
        )
        .unwrap_err();

        assert_eq!(request_error.into_message(), "transient request failure");
        assert_eq!(request_transport.attempts.get(), 4);
        assert_eq!(
            &*request_transport.offsets.borrow(),
            &[None, None, None, None]
        );
        assert_eq!(request_notifications.borrow().len(), 3);
        assert_eq!(&*request_notifications.borrow(), &*request_sleeps.borrow());
        assert!(
            request_notifications
                .borrow()
                .iter()
                .all(|delay| *delay > std::time::Duration::ZERO)
        );
        assert!(std::fs::read_dir(request_dir.path())
            .unwrap()
            .next()
            .is_none());

        let body_dir = tempdir().unwrap();
        let body_transport = FakeTransport {
            responses: RefCell::new(vec![
                failing_transfer(StatusCode::OK, None, b"abc"),
                failing_transfer(
                    StatusCode::PARTIAL_CONTENT,
                    Some("bytes 3-5/6"),
                    b"",
                ),
                failing_transfer(
                    StatusCode::PARTIAL_CONTENT,
                    Some("bytes 3-5/6"),
                    b"",
                ),
                failing_transfer(
                    StatusCode::PARTIAL_CONTENT,
                    Some("bytes 3-5/6"),
                    b"",
                ),
            ]),
            offsets: RefCell::new(Vec::new()),
        };
        let body_notifications = RefCell::new(Vec::new());
        let body_sleeps = RefCell::new(Vec::new());

        let body_error = download_with_transport_controlled(
            &spec(b"abcdef"),
            body_dir.path(),
            &body_transport,
            || false,
            |_| {},
            RetryWait { observer: |delay| body_notifications.borrow_mut().push(delay), sleep: |delay| {
                body_sleeps.borrow_mut().push(delay);
                std::future::ready(())
            } },
            perform_artifact_operation,
        )
        .unwrap_err();

        assert_eq!(body_error.into_message(), "artifact response body failed");
        assert_eq!(
            &*body_transport.offsets.borrow(),
            &[None, Some(3), Some(3), Some(3)]
        );
        assert_eq!(body_notifications.borrow().len(), 3);
        assert_eq!(&*body_notifications.borrow(), &*body_sleeps.borrow());
        assert!(
            body_notifications
                .borrow()
                .iter()
                .all(|delay| *delay > std::time::Duration::ZERO)
        );
        assert_eq!(
            std::fs::read(body_dir.path().join("model.gguf.part")).unwrap(),
            b"abc"
        );
        assert!(!body_dir.path().join("model.gguf").exists());
        assert!(!body_dir.path().join("model.gguf.part.restart").exists());

        let fatal_dir = tempdir().unwrap();
        let fatal_transport = FailingTransport {
            remaining_failures: Cell::new(usize::MAX),
            retryable: false,
            attempts: Cell::new(0),
            offsets: RefCell::new(Vec::new()),
            bytes: b"abcdef".to_vec(),
        };
        let fatal_notifications = Cell::new(0);
        let fatal_sleeps = Cell::new(0);

        let fatal_error = download_with_transport_controlled(
            &spec(b"abcdef"),
            fatal_dir.path(),
            &fatal_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| fatal_notifications.set(fatal_notifications.get() + 1), sleep: |_| {
                fatal_sleeps.set(fatal_sleeps.get() + 1);
                std::future::ready(())
            } },
            perform_artifact_operation,
        )
        .unwrap_err();

        assert_eq!(fatal_error.into_message(), "fatal request failure");
        assert_eq!(fatal_transport.attempts.get(), 1);
        assert_eq!(&*fatal_transport.offsets.borrow(), &[None]);
        assert_eq!(fatal_notifications.get(), 0);
        assert_eq!(fatal_sleeps.get(), 0);
        assert!(std::fs::read_dir(fatal_dir.path())
            .unwrap()
            .next()
            .is_none());
    }

    #[test]
    fn pause_during_pre_admission_artifact_plan_hash_returns_interrupted_without_mutation() {
        let size = 3 * 64 * 1024 + 17;
        let bytes = (0..size).map(|index| (index % 251) as u8).collect::<Vec<_>>();
        let sha256 = hex(Sha256::digest(&bytes).as_ref());

        for name in ["model.gguf", "model.gguf.part"] {
            let root = tempdir().unwrap();
            std::fs::write(root.path().join(name), &bytes).unwrap();
            let directory = crate::safe_file::open_directory(root.path()).unwrap().0;
            let before = artifact_snapshot(root.path());
            let control_checks = Cell::new(0);

            let outcome = plan::plan_artifact_transfer(
                &directory,
                root.path(),
                bytes.len() as u64,
                &sha256,
                &|| {
                    let next = control_checks.get() + 1;
                    control_checks.set(next);
                    next >= 3
                },
            );

            assert!(matches!(outcome, plan::ArtifactPlanOutcome::Interrupted));
            assert!(control_checks.get() >= 3);
            assert_eq!(artifact_snapshot(root.path()), before);
        }
    }

    #[test]
    fn pause_during_hash_stops_before_promotion_and_preserves_exact_prefix() {
        let size = 3 * 64 * 1024 + 17;
        let bytes = (0..size).map(|index| (index % 251) as u8).collect::<Vec<_>>();
        let dir = tempdir().unwrap();
        let transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, &bytes)]),
            offsets: RefCell::new(Vec::new()),
        };
        let verifying = Cell::new(false);
        let hash_checks = Cell::new(0);
        let retry_notifications = Cell::new(0);

        let outcome = download_with_transport_controlled(
            &spec(&bytes),
            dir.path(),
            &transport,
            || {
                if !verifying.get() {
                    return false;
                }
                let next = hash_checks.get() + 1;
                hash_checks.set(next);
                next >= 3
            },
            |update| {
                if matches!(update, ProgressUpdate::Verifying { .. }) {
                    verifying.set(true);
                }
            },
            RetryWait { observer: |_| retry_notifications.set(retry_notifications.get() + 1), sleep: tokio::time::sleep },
            perform_artifact_operation,
        )
        .unwrap();

        assert!(matches!(
            outcome,
            DownloadTerminalOutcome::Paused { retained_bytes }
                if retained_bytes == bytes.len() as u64
        ));
        assert!(hash_checks.get() >= 3);
        assert_eq!(&*transport.offsets.borrow(), &[None]);
        assert_eq!(retry_notifications.get(), 0);
        assert_eq!(
            std::fs::read(dir.path().join("model.gguf.part")).unwrap(),
            bytes
        );
        assert!(!dir.path().join("model.gguf").exists());
        assert!(!dir.path().join("model.gguf.part.restart").exists());
    }

    #[test]
    fn pause_immediately_before_promotion_returns_only_after_sync_and_restat() {
        let bytes = b"abcdef";
        let dir = tempdir().unwrap();
        let transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, bytes)]),
            offsets: RefCell::new(Vec::new()),
        };
        let verifying = Cell::new(false);
        let verification_checks = Cell::new(0);
        let checkpoints = RefCell::new(Vec::new());

        let outcome = download_with_transport_controlled(
            &spec(bytes),
            dir.path(),
            &transport,
            || {
                if !verifying.get() {
                    return false;
                }
                let next = verification_checks.get() + 1;
                verification_checks.set(next);
                next >= 3
            },
            |update| {
                if matches!(update, ProgressUpdate::Verifying { .. }) {
                    verifying.set(true);
                }
            },
            RetryWait { observer: |_| {}, sleep: tokio::time::sleep },
            |operation| {
                let checkpoint = match &operation {
                    ArtifactOperation::Sync { checkpoint, .. }
                    | ArtifactOperation::Observe(checkpoint) => Some(*checkpoint),
                    ArtifactOperation::Write { .. } => None,
                };
                if let Some(checkpoint) = checkpoint {
                    checkpoints.borrow_mut().push(checkpoint);
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap();

        assert!(matches!(
            outcome,
            DownloadTerminalOutcome::Paused { retained_bytes: 6 }
        ));
        assert_eq!(verification_checks.get(), 3);
        assert_eq!(
            &*checkpoints.borrow(),
            &[
                ArtifactCheckpoint::StagingSynced,
                ArtifactCheckpoint::BeforePromotion,
                ArtifactCheckpoint::StagingSynced,
                ArtifactCheckpoint::StagingIdentityMatched,
                ArtifactCheckpoint::DirectorySynced,
                ArtifactCheckpoint::NormalizationDirectorySynced,
                ArtifactCheckpoint::AuthoritativeRestatted,
            ]
        );
        assert_eq!(
            std::fs::read(dir.path().join("model.gguf.part")).unwrap(),
            bytes
        );
        assert!(!dir.path().join("model.gguf").exists());
        assert!(!dir.path().join("model.gguf.part.restart").exists());
    }

    #[test]
    fn durable_part_barrier_revalidates_after_authoritative_restat_hook() {
        let bytes = b"abcdef";
        let dir = tempdir().unwrap();
        let part = dir.path().join("model.gguf.part");
        let original = dir.path().join("original-part");
        let transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, bytes)]),
            offsets: RefCell::new(Vec::new()),
        };
        let verifying = Cell::new(false);
        let verification_checks = Cell::new(0);
        let substituted = Cell::new(false);
        let retry_notifications = Cell::new(0);

        let failure = download_with_transport_controlled(
            &spec(bytes),
            dir.path(),
            &transport,
            || {
                if !verifying.get() {
                    return false;
                }
                let next = verification_checks.get() + 1;
                verification_checks.set(next);
                next >= 3
            },
            |update| {
                if matches!(update, ProgressUpdate::Verifying { .. }) {
                    verifying.set(true);
                }
            },
            RetryWait {
                observer: |_| retry_notifications.set(retry_notifications.get() + 1),
                sleep: tokio::time::sleep,
            },
            |operation| {
                if matches!(
                    &operation,
                    ArtifactOperation::Observe(ArtifactCheckpoint::AuthoritativeRestatted)
                ) {
                    std::fs::rename(&part, &original).unwrap();
                    std::fs::write(&part, bytes).unwrap();
                    substituted.set(true);
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();

        assert!(substituted.get());
        assert_eq!(failure, DownloadFailure::Durability);
        assert_eq!(&*transport.offsets.borrow(), &[None]);
        assert_eq!(retry_notifications.get(), 0);
        assert_eq!(std::fs::read(&original).unwrap(), bytes);
        assert_eq!(std::fs::read(&part).unwrap(), bytes);
        assert!(!dir.path().join("model.gguf").exists());
        assert!(!dir.path().join("model.gguf.part.restart").exists());

        let swap_root = tempdir().unwrap();
        let model_dir = swap_root.path().join("model");
        let moved_dir = swap_root.path().join("moved-model");
        std::fs::create_dir(&model_dir).unwrap();
        let swap_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, bytes)]),
            offsets: RefCell::new(Vec::new()),
        };
        let swap_verifying = Cell::new(false);
        let swap_checks = Cell::new(0);
        let swapped = Cell::new(false);
        let swap_retries = Cell::new(0);

        let swap_failure = download_with_transport_controlled(
            &spec(bytes),
            &model_dir,
            &swap_transport,
            || {
                if !swap_verifying.get() {
                    return false;
                }
                let next = swap_checks.get() + 1;
                swap_checks.set(next);
                next >= 3
            },
            |update| {
                if matches!(update, ProgressUpdate::Verifying { .. }) {
                    swap_verifying.set(true);
                }
            },
            RetryWait {
                observer: |_| swap_retries.set(swap_retries.get() + 1),
                sleep: tokio::time::sleep,
            },
            |operation| {
                if matches!(
                    &operation,
                    ArtifactOperation::Observe(ArtifactCheckpoint::AuthoritativeRestatted)
                ) {
                    std::fs::rename(&model_dir, &moved_dir).unwrap();
                    std::fs::create_dir(&model_dir).unwrap();
                    std::fs::write(model_dir.join("model.gguf.part"), b"ghijkl").unwrap();
                    swapped.set(true);
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();

        assert!(swapped.get());
        assert_eq!(swap_failure, DownloadFailure::Durability);
        assert_eq!(&*swap_transport.offsets.borrow(), &[None]);
        assert_eq!(swap_retries.get(), 0);
        assert_eq!(std::fs::read(moved_dir.join("model.gguf.part")).unwrap(), bytes);
        assert_eq!(std::fs::read(model_dir.join("model.gguf.part")).unwrap(), b"ghijkl");
        assert!(!moved_dir.join("model.gguf").exists());
        assert!(!model_dir.join("model.gguf").exists());
    }

    #[test]
    fn pause_after_completion_fence_loses_to_exact_promotion() {
        let bytes = b"abcdef";
        let dir = tempdir().unwrap();
        let transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, bytes)]),
            offsets: RefCell::new(Vec::new()),
        };
        let verifying = Cell::new(false);
        let completion_fence_passed = Cell::new(false);
        let verification_checks = Cell::new(0);
        let checkpoints = RefCell::new(Vec::new());

        let outcome = download_with_transport_controlled(
            &spec(bytes),
            dir.path(),
            &transport,
            || {
                if !verifying.get() {
                    return false;
                }
                verification_checks.set(verification_checks.get() + 1);
                completion_fence_passed.get()
            },
            |update| {
                if matches!(update, ProgressUpdate::Verifying { .. }) {
                    verifying.set(true);
                }
            },
            RetryWait { observer: |_| {}, sleep: tokio::time::sleep },
            |operation| {
                let checkpoint = match &operation {
                    ArtifactOperation::Sync { checkpoint, .. }
                    | ArtifactOperation::Observe(checkpoint) => Some(*checkpoint),
                    ArtifactOperation::Write { .. } => None,
                };
                if let Some(checkpoint) = checkpoint {
                    checkpoints.borrow_mut().push(checkpoint);
                    if checkpoint == ArtifactCheckpoint::CompletionFencePassed {
                        completion_fence_passed.set(true);
                    }
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap();

        assert!(matches!(
            outcome,
            DownloadTerminalOutcome::Complete(DownloadOutcome::Pulled(ref path))
                if path == &dir.path().join("model.gguf")
        ));
        assert!(completion_fence_passed.get());
        assert_eq!(verification_checks.get(), 3);
        assert_eq!(
            &*checkpoints.borrow(),
            &[
                ArtifactCheckpoint::StagingSynced,
                ArtifactCheckpoint::BeforePromotion,
                ArtifactCheckpoint::CompletionFencePassed,
                ArtifactCheckpoint::PromotionDirectorySynced,
            ]
        );
        assert_eq!(
            std::fs::read(dir.path().join("model.gguf")).unwrap(),
            bytes
        );
        assert!(!dir.path().join("model.gguf.part").exists());
        assert!(!dir.path().join("model.gguf.part.restart").exists());
    }

    #[test]
    fn pause_sync_failure_is_durability_not_paused_and_claims_no_retained_bytes() {
        let bytes = b"abcdef";
        let failure_points = [
            (ArtifactCheckpoint::StagingSynced, 1, true),
            (ArtifactCheckpoint::StagingSynced, 2, true),
            (ArtifactCheckpoint::StagingIdentityMatched, 1, false),
            (ArtifactCheckpoint::DirectorySynced, 1, true),
            (
                ArtifactCheckpoint::NormalizationDirectorySynced,
                1,
                true,
            ),
            (ArtifactCheckpoint::AuthoritativeRestatted, 1, false),
        ];

        for (failure_checkpoint, failure_occurrence, failure_is_sync) in failure_points {
            let dir = tempdir().unwrap();
            let transport = FakeTransport {
                responses: RefCell::new(vec![transfer(StatusCode::OK, None, bytes)]),
                offsets: RefCell::new(Vec::new()),
            };
            let verifying = Cell::new(false);
            let verification_checks = Cell::new(0);
            let matching_operations = Cell::new(0);
            let operations = RefCell::new(Vec::new());
            let retry_notifications = Cell::new(0);

            let failure = download_with_transport_controlled(
                &spec(bytes),
                dir.path(),
                &transport,
                || {
                    if !verifying.get() {
                        return false;
                    }
                    let next = verification_checks.get() + 1;
                    verification_checks.set(next);
                    next >= 3
                },
                |update| {
                    if matches!(update, ProgressUpdate::Verifying { .. }) {
                        verifying.set(true);
                    }
                },
                RetryWait { observer: |_| retry_notifications.set(retry_notifications.get() + 1), sleep: tokio::time::sleep },
                |operation| {
                    match operation {
                        ArtifactOperation::Sync { checkpoint, file } => {
                            operations.borrow_mut().push(checkpoint);
                            if failure_is_sync && checkpoint == failure_checkpoint {
                                let next = matching_operations.get() + 1;
                                matching_operations.set(next);
                                if next == failure_occurrence {
                                    return Err(ArtifactOperationFailure::Other);
                                }
                            }
                            file.sync_all()
                                .map_err(|_| ArtifactOperationFailure::Other)
                        }
                        ArtifactOperation::Write { file, bytes } => file
                            .write_all(bytes)
                            .map_err(|_| ArtifactOperationFailure::Other),
                        ArtifactOperation::Observe(checkpoint) => {
                            operations.borrow_mut().push(checkpoint);
                            if !failure_is_sync && checkpoint == failure_checkpoint {
                                let next = matching_operations.get() + 1;
                                matching_operations.set(next);
                                if next == failure_occurrence {
                                    return Err(ArtifactOperationFailure::Other);
                                }
                            }
                            Ok(())
                        }
                    }
                },
            )
            .unwrap_err();

            assert_eq!(failure, DownloadFailure::Durability);
            assert_eq!(retry_notifications.get(), 0);
            assert_eq!(&*transport.offsets.borrow(), &[None]);
            assert!(operations.borrow().contains(&failure_checkpoint));
            assert_eq!(operations.borrow().last(), Some(&failure_checkpoint));
            assert_eq!(
                std::fs::read(dir.path().join("model.gguf.part")).unwrap(),
                bytes
            );
            assert!(!dir.path().join("model.gguf").exists());
            assert!(!dir.path().join("model.gguf.part.restart").exists());
        }
    }

    #[test]
    fn local_enospc_is_not_retried_and_exposes_only_durably_recovered_prefix() {
        #[derive(Debug, Eq, PartialEq)]
        enum RecordedOperation {
            Write,
            Checkpoint(ArtifactCheckpoint),
        }

        let bytes = b"abcdef";
        for fail_normalization_sync in [false, true] {
            let dir = tempdir().unwrap();
            let transport = FakeTransport {
                responses: RefCell::new(vec![transfer(StatusCode::OK, None, bytes)]),
                offsets: RefCell::new(Vec::new()),
            };
            let retry_notifications = Cell::new(0);
            let progress = RefCell::new(Vec::new());
            let operations = RefCell::new(Vec::new());

            let failure = download_with_transport_controlled(
                &spec(bytes),
                dir.path(),
                &transport,
                || false,
                |update| progress.borrow_mut().push(update),
                RetryWait { observer: |_| retry_notifications.set(retry_notifications.get() + 1), sleep: tokio::time::sleep },
                |operation| match operation {
                    ArtifactOperation::Write { file, bytes } => {
                        operations.borrow_mut().push(RecordedOperation::Write);
                        file.write_all(&bytes[..3]).unwrap();
                        Err(ArtifactOperationFailure::DiskExhausted)
                    }
                    ArtifactOperation::Sync { checkpoint, file } => {
                        operations
                            .borrow_mut()
                            .push(RecordedOperation::Checkpoint(checkpoint));
                        if fail_normalization_sync
                            && checkpoint == ArtifactCheckpoint::NormalizationDirectorySynced
                        {
                            return Err(ArtifactOperationFailure::Other);
                        }
                        file.sync_all()
                            .map_err(|_| ArtifactOperationFailure::Other)
                    }
                    ArtifactOperation::Observe(checkpoint) => {
                        operations
                            .borrow_mut()
                            .push(RecordedOperation::Checkpoint(checkpoint));
                        Ok(())
                    }
                },
            )
            .unwrap_err();

            if fail_normalization_sync {
                assert_eq!(failure, DownloadFailure::Durability);
                assert_eq!(
                    &*operations.borrow(),
                    &[
                        RecordedOperation::Write,
                        RecordedOperation::Checkpoint(ArtifactCheckpoint::StagingSynced),
                        RecordedOperation::Checkpoint(
                            ArtifactCheckpoint::StagingIdentityMatched,
                        ),
                        RecordedOperation::Checkpoint(ArtifactCheckpoint::DirectorySynced),
                        RecordedOperation::Checkpoint(
                            ArtifactCheckpoint::NormalizationDirectorySynced,
                        ),
                    ]
                );
            } else {
                assert_eq!(
                    failure,
                    DownloadFailure::DiskExhausted { retained_bytes: 3 }
                );
                assert_eq!(
                    &*operations.borrow(),
                    &[
                        RecordedOperation::Write,
                        RecordedOperation::Checkpoint(ArtifactCheckpoint::StagingSynced),
                        RecordedOperation::Checkpoint(
                            ArtifactCheckpoint::StagingIdentityMatched,
                        ),
                        RecordedOperation::Checkpoint(ArtifactCheckpoint::DirectorySynced),
                        RecordedOperation::Checkpoint(
                            ArtifactCheckpoint::NormalizationDirectorySynced,
                        ),
                        RecordedOperation::Checkpoint(
                            ArtifactCheckpoint::AuthoritativeRestatted,
                        ),
                    ]
                );
            }
            assert_eq!(&*transport.offsets.borrow(), &[None]);
            assert_eq!(retry_notifications.get(), 0);
            assert_eq!(
                &*progress.borrow(),
                &[ProgressUpdate::Transferring {
                    transferred: 0,
                    total: 6,
                }]
            );
            assert_eq!(
                std::fs::read(dir.path().join("model.gguf.part")).unwrap(),
                b"abc"
            );
            assert!(!dir.path().join("model.gguf").exists());
            assert!(!dir.path().join("model.gguf.part.restart").exists());
        }

        let dir = tempdir().unwrap();
        let transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, bytes)]),
            offsets: RefCell::new(Vec::new()),
        };
        let retry_notifications = Cell::new(0);
        let operations = RefCell::new(Vec::new());
        let failure = download_with_transport_controlled(
            &spec(bytes),
            dir.path(),
            &transport,
            || false,
            |_| {},
            RetryWait { observer: |_| retry_notifications.set(retry_notifications.get() + 1), sleep: tokio::time::sleep },
            |operation| match operation {
                ArtifactOperation::Write { file, bytes } => {
                    operations.borrow_mut().push(RecordedOperation::Write);
                    file.write_all(&bytes[..3]).unwrap();
                    Err(ArtifactOperationFailure::Other)
                }
                ArtifactOperation::Sync { checkpoint, file } => {
                    operations
                        .borrow_mut()
                        .push(RecordedOperation::Checkpoint(checkpoint));
                    file.sync_all()
                        .map_err(|_| ArtifactOperationFailure::Other)
                }
                ArtifactOperation::Observe(checkpoint) => {
                    operations
                        .borrow_mut()
                        .push(RecordedOperation::Checkpoint(checkpoint));
                    Ok(())
                }
            },
        )
        .unwrap_err();

        assert_eq!(failure, DownloadFailure::Durability);
        assert_eq!(&*operations.borrow(), &[RecordedOperation::Write]);
        assert_eq!(&*transport.offsets.borrow(), &[None]);
        assert_eq!(retry_notifications.get(), 0);
        assert_eq!(
            std::fs::read(dir.path().join("model.gguf.part")).unwrap(),
            b"abc"
        );
        assert!(!dir.path().join("model.gguf").exists());
        assert!(!dir.path().join("model.gguf.part.restart").exists());
    }

    #[test]
    fn terminal_body_error_normalizes_and_reports_only_a_durable_prefix() {
        let bytes = b"abcdef";
        let dir = tempdir().unwrap();
        let transport = FakeTransport {
            responses: RefCell::new(vec![
                failing_transfer(StatusCode::OK, None, b"abc"),
                failing_transfer(StatusCode::PARTIAL_CONTENT, Some("bytes 3-5/6"), b""),
                failing_transfer(StatusCode::PARTIAL_CONTENT, Some("bytes 3-5/6"), b""),
                failing_transfer(StatusCode::PARTIAL_CONTENT, Some("bytes 3-5/6"), b""),
            ]),
            offsets: RefCell::new(Vec::new()),
        };
        let retry_notifications = RefCell::new(Vec::new());
        let authoritative_restats = Cell::new(0);

        let failure = download_with_transport_controlled(
            &spec(bytes),
            dir.path(),
            &transport,
            || false,
            |_| {},
            RetryWait { observer: |delay| retry_notifications.borrow_mut().push(delay), sleep: |_| std::future::ready(()) },
            |operation| {
                if let ArtifactOperation::Observe(checkpoint) = &operation {
                    if *checkpoint == ArtifactCheckpoint::AuthoritativeRestatted {
                        authoritative_restats.set(authoritative_restats.get() + 1);
                    }
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();

        assert_eq!(failure, DownloadFailure::Remote { retained_bytes: 3 });
        assert_eq!(
            &*transport.offsets.borrow(),
            &[None, Some(3), Some(3), Some(3)]
        );
        assert_eq!(retry_notifications.borrow().len(), 3);
        assert!(
            retry_notifications
                .borrow()
                .iter()
                .all(|delay| *delay > std::time::Duration::ZERO)
        );
        assert_eq!(authoritative_restats.get(), 4);
        assert_eq!(
            std::fs::read(dir.path().join("model.gguf.part")).unwrap(),
            b"abc"
        );
        assert!(!dir.path().join("model.gguf").exists());
        assert!(!dir.path().join("model.gguf.part.restart").exists());

        let pause_dir = tempdir().unwrap();
        let pause_transport = FakeTransport {
            responses: RefCell::new(vec![failing_transfer(StatusCode::OK, None, b"abc")]),
            offsets: RefCell::new(Vec::new()),
        };
        let pause = Cell::new(false);
        let pause_retries = Cell::new(0);
        let paused = download_with_transport_controlled(
            &spec(bytes),
            pause_dir.path(),
            &pause_transport,
            || pause.get(),
            |_| {},
            RetryWait { observer: |_| pause_retries.set(pause_retries.get() + 1), sleep: |_| {
                pause.set(true);
                std::future::ready(())
            } },
            perform_artifact_operation,
        )
        .unwrap();

        assert_eq!(
            paused,
            DownloadTerminalOutcome::Paused { retained_bytes: 3 }
        );
        assert_eq!(pause_retries.get(), 1);
        assert_eq!(&*pause_transport.offsets.borrow(), &[None]);
        assert_eq!(
            std::fs::read(pause_dir.path().join("model.gguf.part")).unwrap(),
            b"abc"
        );
        assert!(!pause_dir.path().join("model.gguf").exists());
        assert!(!pause_dir.path().join("model.gguf.part.restart").exists());

        let durability_dir = tempdir().unwrap();
        let durability_transport = FakeTransport {
            responses: RefCell::new(vec![failing_transfer(StatusCode::OK, None, b"abc")]),
            offsets: RefCell::new(Vec::new()),
        };
        let durability_retries = Cell::new(0);
        let operations = RefCell::new(Vec::new());
        let durability = download_with_transport_controlled(
            &spec(bytes),
            durability_dir.path(),
            &durability_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| durability_retries.set(durability_retries.get() + 1), sleep: |_| std::future::ready(()) },
            |operation| match operation {
                ArtifactOperation::Sync { checkpoint, file } => {
                    operations.borrow_mut().push(checkpoint);
                    if checkpoint == ArtifactCheckpoint::NormalizationDirectorySynced {
                        return Err(ArtifactOperationFailure::Other);
                    }
                    file.sync_all()
                        .map_err(|_| ArtifactOperationFailure::Other)
                }
                ArtifactOperation::Write { file, bytes } => file
                    .write_all(bytes)
                    .map_err(|_| ArtifactOperationFailure::Other),
                ArtifactOperation::Observe(checkpoint) => {
                    operations.borrow_mut().push(checkpoint);
                    Ok(())
                }
            },
        )
        .unwrap_err();

        assert_eq!(durability, DownloadFailure::Durability);
        assert_eq!(durability_retries.get(), 0);
        assert_eq!(&*durability_transport.offsets.borrow(), &[None]);
        assert_eq!(
            operations.borrow().last(),
            Some(&ArtifactCheckpoint::NormalizationDirectorySynced)
        );
        assert_eq!(
            std::fs::read(durability_dir.path().join("model.gguf.part")).unwrap(),
            b"abc"
        );
        assert!(!durability_dir.path().join("model.gguf").exists());
        assert!(!durability_dir
            .path()
            .join("model.gguf.part.restart")
            .exists());
    }

    #[test]
    fn proven_body_prefix_survives_later_request_pause() {
        let bytes = b"abcdef";
        let dir = tempdir().unwrap();
        let pause = Cell::new(false);
        let transport = BodyThenPausingRequestTransport {
            pause: &pause,
            attempts: Cell::new(0),
            offsets: RefCell::new(Vec::new()),
            first: RefCell::new(Some(failing_transfer(StatusCode::OK, None, b"abc"))),
        };
        let retry_notifications = Cell::new(0);

        let outcome = download_with_transport_controlled(
            &spec(bytes),
            dir.path(),
            &transport,
            || pause.get(),
            |_| {},
            RetryWait { observer: |_| retry_notifications.set(retry_notifications.get() + 1), sleep: |_| std::future::ready(()) },
            perform_artifact_operation,
        )
        .unwrap();

        assert_eq!(
            outcome,
            DownloadTerminalOutcome::Paused { retained_bytes: 3 }
        );
        assert_eq!(transport.attempts.get(), 2);
        assert_eq!(&*transport.offsets.borrow(), &[None, Some(3)]);
        assert_eq!(retry_notifications.get(), 1);
        assert_eq!(
            std::fs::read(dir.path().join("model.gguf.part")).unwrap(),
            b"abc"
        );
        assert!(!dir.path().join("model.gguf").exists());
        assert!(!dir.path().join("model.gguf.part.restart").exists());
    }

    #[test]
    fn proven_body_prefix_survives_later_fatal_or_exhausted_request() {
        let bytes = b"abcdef";
        for (retryable, expected_attempts, expected_retries) in
            [(false, 2, 1), (true, 4, 3)]
        {
            let dir = tempdir().unwrap();
            let transport = BodyThenFailingRequestTransport {
                attempts: Cell::new(0),
                offsets: RefCell::new(Vec::new()),
                first: RefCell::new(Some(failing_transfer(StatusCode::OK, None, b"abc"))),
                retryable,
            };
            let retry_notifications = Cell::new(0);

            let failure = download_with_transport_controlled(
                &spec(bytes),
                dir.path(),
                &transport,
                || false,
                |_| {},
                RetryWait { observer: |_| retry_notifications.set(retry_notifications.get() + 1), sleep: |_| std::future::ready(()) },
                perform_artifact_operation,
            )
            .unwrap_err();

            assert_eq!(failure, DownloadFailure::Remote { retained_bytes: 3 });
            assert_eq!(transport.attempts.get(), expected_attempts);
            assert_eq!(retry_notifications.get(), expected_retries);
            assert_eq!(
                &*transport.offsets.borrow(),
                &vec![None]
                    .into_iter()
                    .chain(std::iter::repeat_n(
                        Some(3),
                        expected_attempts.saturating_sub(1),
                    ))
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                std::fs::read(dir.path().join("model.gguf.part")).unwrap(),
                b"abc"
            );
            assert!(!dir.path().join("model.gguf").exists());
            assert!(!dir.path().join("model.gguf.part.restart").exists());
        }
    }

    #[test]
    fn short_body_eof_uses_full_durability_barrier_before_remote_recovery() {
        let bytes = b"abcdef";
        let durability_dir = tempdir().unwrap();
        let durability_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, b"abc")]),
            offsets: RefCell::new(Vec::new()),
        };
        let durability_retries = Cell::new(0);
        let operations = RefCell::new(Vec::new());

        let durability = download_with_transport_controlled(
            &spec(bytes),
            durability_dir.path(),
            &durability_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| durability_retries.set(durability_retries.get() + 1), sleep: |_| std::future::ready(()) },
            |operation| match operation {
                ArtifactOperation::Sync { checkpoint, file } => {
                    operations.borrow_mut().push(checkpoint);
                    if checkpoint == ArtifactCheckpoint::NormalizationDirectorySynced {
                        return Err(ArtifactOperationFailure::Other);
                    }
                    file.sync_all()
                        .map_err(|_| ArtifactOperationFailure::Other)
                }
                ArtifactOperation::Write { file, bytes } => file
                    .write_all(bytes)
                    .map_err(|_| ArtifactOperationFailure::Other),
                ArtifactOperation::Observe(checkpoint) => {
                    operations.borrow_mut().push(checkpoint);
                    Ok(())
                }
            },
        )
        .unwrap_err();

        assert_eq!(durability, DownloadFailure::Durability);
        assert_eq!(durability_retries.get(), 0);
        assert_eq!(&*durability_transport.offsets.borrow(), &[None]);
        assert_eq!(
            operations.borrow().last(),
            Some(&ArtifactCheckpoint::NormalizationDirectorySynced)
        );
        assert_eq!(
            std::fs::read(durability_dir.path().join("model.gguf.part")).unwrap(),
            b"abc"
        );
        assert!(!durability_dir.path().join("model.gguf").exists());
        assert!(!durability_dir
            .path()
            .join("model.gguf.part.restart")
            .exists());

        let dir = tempdir().unwrap();
        let transport = FakeTransport {
            responses: RefCell::new(vec![
                transfer(StatusCode::OK, None, b"abc"),
                transfer(StatusCode::PARTIAL_CONTENT, Some("bytes 3-5/6"), b""),
                transfer(StatusCode::PARTIAL_CONTENT, Some("bytes 3-5/6"), b""),
                transfer(StatusCode::PARTIAL_CONTENT, Some("bytes 3-5/6"), b""),
            ]),
            offsets: RefCell::new(Vec::new()),
        };
        let retry_notifications = Cell::new(0);
        let authoritative_restats = Cell::new(0);

        let failure = download_with_transport_controlled(
            &spec(bytes),
            dir.path(),
            &transport,
            || false,
            |_| {},
            RetryWait { observer: |_| retry_notifications.set(retry_notifications.get() + 1), sleep: |_| std::future::ready(()) },
            |operation| {
                if let ArtifactOperation::Observe(checkpoint) = &operation {
                    if *checkpoint == ArtifactCheckpoint::AuthoritativeRestatted {
                        authoritative_restats.set(authoritative_restats.get() + 1);
                    }
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();

        assert_eq!(failure, DownloadFailure::Remote { retained_bytes: 3 });
        assert_eq!(
            &*transport.offsets.borrow(),
            &[None, Some(3), Some(3), Some(3)]
        );
        assert_eq!(retry_notifications.get(), 3);
        assert_eq!(authoritative_restats.get(), 4);
        assert_eq!(
            std::fs::read(dir.path().join("model.gguf.part")).unwrap(),
            b"abc"
        );
        assert!(!dir.path().join("model.gguf").exists());
        assert!(!dir.path().join("model.gguf.part.restart").exists());
    }

    #[test]
    fn proven_zero_body_prefix_survives_later_request_failure() {
        let bytes = b"abcdef";
        for (retryable, expected_attempts, expected_retries) in
            [(false, 2, 1), (true, 4, 3)]
        {
            let dir = tempdir().unwrap();
            let transport = BodyThenFailingRequestTransport {
                attempts: Cell::new(0),
                offsets: RefCell::new(Vec::new()),
                first: RefCell::new(Some(failing_transfer(StatusCode::OK, None, b""))),
                retryable,
            };
            let retry_notifications = Cell::new(0);

            let failure = download_with_transport_controlled(
                &spec(bytes),
                dir.path(),
                &transport,
                || false,
                |_| {},
                RetryWait { observer: |_| retry_notifications.set(retry_notifications.get() + 1), sleep: |_| std::future::ready(()) },
                perform_artifact_operation,
            )
            .unwrap_err();

            assert_eq!(failure, DownloadFailure::Remote { retained_bytes: 0 });
            assert_eq!(transport.attempts.get(), expected_attempts);
            assert_eq!(retry_notifications.get(), expected_retries);
            assert_eq!(
                &*transport.offsets.borrow(),
                &std::iter::repeat_n(None, expected_attempts).collect::<Vec<_>>()
            );
            let part = dir.path().join("model.gguf.part");
            assert!(part.exists());
            assert!(std::fs::read(part).unwrap().is_empty());
            assert!(!dir.path().join("model.gguf").exists());
            assert!(!dir.path().join("model.gguf.part.restart").exists());
        }
    }

    #[test]
    fn proven_remote_prefix_survives_later_status_or_content_range_failure() {
        let bytes = b"abcdef";
        for later_response in [
            transfer(StatusCode::BAD_REQUEST, None, b""),
            transfer(
                StatusCode::PARTIAL_CONTENT,
                Some("bytes 4-5/6"),
                b"",
            ),
        ] {
            let dir = tempdir().unwrap();
            let transport = FakeTransport {
                responses: RefCell::new(vec![
                    failing_transfer(StatusCode::OK, None, b"abc"),
                    later_response,
                ]),
                offsets: RefCell::new(Vec::new()),
            };
            let retry_notifications = Cell::new(0);

            let failure = download_with_transport_controlled(
                &spec(bytes),
                dir.path(),
                &transport,
                || false,
                |_| {},
                RetryWait { observer: |_| retry_notifications.set(retry_notifications.get() + 1), sleep: |_| std::future::ready(()) },
                perform_artifact_operation,
            )
            .unwrap_err();

            assert_eq!(failure, DownloadFailure::Remote { retained_bytes: 3 });
            assert_eq!(&*transport.offsets.borrow(), &[None, Some(3)]);
            assert_eq!(retry_notifications.get(), 1);
            assert_eq!(
                std::fs::read(dir.path().join("model.gguf.part")).unwrap(),
                b"abc"
            );
            assert!(!dir.path().join("model.gguf").exists());
            assert!(!dir.path().join("model.gguf.part.restart").exists());
        }
    }

    #[test]
    fn checksum_invalid_authoritative_part_unlinks_syncs_restats_absent_before_integrity() {
        let expected = b"ghijkl";
        let received = b"abcdef";
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("model.gguf.part.restart"), b"keep restart").unwrap();
        std::fs::write(dir.path().join("model.gguf.invalid"), b"keep invalid").unwrap();
        std::fs::write(dir.path().join("foreign.bin"), b"keep foreign").unwrap();
        let before = artifact_snapshot(dir.path());
        let transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, received)]),
            offsets: RefCell::new(Vec::new()),
        };
        let retry_notifications = Cell::new(0);
        let checkpoints = RefCell::new(Vec::new());

        let failure = download_with_transport_controlled(
            &spec(expected),
            dir.path(),
            &transport,
            || false,
            |_| {},
            RetryWait { observer: |_| retry_notifications.set(retry_notifications.get() + 1), sleep: |_| std::future::ready(()) },
            |operation| {
                match &operation {
                    ArtifactOperation::Sync { checkpoint, .. }
                    | ArtifactOperation::Observe(checkpoint) => {
                        checkpoints.borrow_mut().push(*checkpoint)
                    }
                    ArtifactOperation::Write { .. } => {}
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();

        assert_eq!(
            failure,
            DownloadFailure::Integrity {
                retained_bytes: 0,
                authority: IntegrityAuthority::Repair,
            }
        );
        assert_eq!(retry_notifications.get(), 0);
        assert_eq!(&*transport.offsets.borrow(), &[None]);
        assert_eq!(
            &*checkpoints.borrow(),
            &[
                ArtifactCheckpoint::StagingSynced,
                ArtifactCheckpoint::BeforeAuthoritativePartUnlink,
                ArtifactCheckpoint::AuthoritativePartIdentityMatched,
                ArtifactCheckpoint::AuthoritativePartUnlinked,
                ArtifactCheckpoint::ChecksumCleanupDirectorySynced,
                ArtifactCheckpoint::BeforeAuthoritativePartAbsenceProof,
                ArtifactCheckpoint::AuthoritativePartAbsent,
                ArtifactCheckpoint::BeforeIntegrityAuthorityObservation,
            ]
        );
        assert_eq!(artifact_snapshot(dir.path()), before);
        assert!(!dir.path().join("model.gguf").exists());
        assert!(!dir.path().join("model.gguf.part").exists());

        let substitution_dir = tempdir().unwrap();
        let substitution_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, received)]),
            offsets: RefCell::new(Vec::new()),
        };
        let substitution_retries = Cell::new(0);
        let substitution_checkpoints = RefCell::new(Vec::new());
        let substituted = Cell::new(false);
        let part = substitution_dir.path().join("model.gguf.part");
        let original = substitution_dir.path().join("substituted-original");

        let substitution = download_with_transport_controlled(
            &spec(expected),
            substitution_dir.path(),
            &substitution_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| substitution_retries.set(substitution_retries.get() + 1), sleep: |_| std::future::ready(()) },
            |operation| {
                match &operation {
                    ArtifactOperation::Sync { checkpoint, .. }
                    | ArtifactOperation::Observe(checkpoint) => {
                        substitution_checkpoints.borrow_mut().push(*checkpoint);
                        if *checkpoint == ArtifactCheckpoint::AuthoritativePartIdentityMatched {
                            std::fs::rename(&part, &original).unwrap();
                            std::fs::write(&part, b"replacement").unwrap();
                            substituted.set(true);
                        }
                    }
                    ArtifactOperation::Write { .. } => {}
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();

        assert!(substituted.get());
        assert_eq!(substitution, DownloadFailure::Durability);
        assert_eq!(substitution_retries.get(), 0);
        assert_eq!(&*substitution_transport.offsets.borrow(), &[None]);
        assert_eq!(
            substitution_checkpoints.borrow().last(),
            Some(&ArtifactCheckpoint::AuthoritativePartIdentityMatched)
        );
        assert_eq!(std::fs::read(&part).unwrap(), b"replacement");
        assert_eq!(std::fs::read(&original).unwrap(), received);
        assert!(!substitution_dir.path().join("model.gguf").exists());
        assert!(!substitution_dir
            .path()
            .join("model.gguf.part.restart")
            .exists());

        let directory_swap_root = tempdir().unwrap();
        let model_dir = directory_swap_root.path().join("model");
        let moved_dir = directory_swap_root.path().join("moved-model");
        std::fs::create_dir(&model_dir).unwrap();
        let directory_swap_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, received)]),
            offsets: RefCell::new(Vec::new()),
        };
        let directory_swap_retries = Cell::new(0);
        let directory_swap_checkpoints = RefCell::new(Vec::new());
        let directory_swapped = Cell::new(false);

        let directory_swap = download_with_transport_controlled(
            &spec(expected),
            &model_dir,
            &directory_swap_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| directory_swap_retries.set(directory_swap_retries.get() + 1), sleep: |_| std::future::ready(()) },
            |operation| {
                match &operation {
                    ArtifactOperation::Sync { checkpoint, .. }
                    | ArtifactOperation::Observe(checkpoint) => {
                        directory_swap_checkpoints.borrow_mut().push(*checkpoint);
                        if *checkpoint == ArtifactCheckpoint::AuthoritativePartIdentityMatched {
                            std::fs::rename(&model_dir, &moved_dir).unwrap();
                            std::fs::create_dir(&model_dir).unwrap();
                            std::fs::write(model_dir.join("model.gguf.part"), b"replacement")
                                .unwrap();
                            directory_swapped.set(true);
                        }
                    }
                    ArtifactOperation::Write { .. } => {}
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();

        assert!(directory_swapped.get());
        assert_eq!(directory_swap, DownloadFailure::Durability);
        assert_eq!(directory_swap_retries.get(), 0);
        assert_eq!(&*directory_swap_transport.offsets.borrow(), &[None]);
        assert_eq!(
            directory_swap_checkpoints.borrow().last(),
            Some(&ArtifactCheckpoint::ChecksumCleanupDirectorySynced)
        );
        assert_eq!(
            std::fs::read(model_dir.join("model.gguf.part")).unwrap(),
            b"replacement"
        );
        assert!(!moved_dir.join("model.gguf.part").exists());
        assert!(!model_dir.join("model.gguf").exists());
        assert!(!moved_dir.join("model.gguf").exists());

        let absence_substitution_dir = tempdir().unwrap();
        let absence_substitution_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, received)]),
            offsets: RefCell::new(Vec::new()),
        };
        let absence_substitution_retries = Cell::new(0);
        let absence_substitution_checkpoints = RefCell::new(Vec::new());
        let absence_substituted = Cell::new(false);
        let absence_part = absence_substitution_dir.path().join("model.gguf.part");

        let absence_substitution = download_with_transport_controlled(
            &spec(expected),
            absence_substitution_dir.path(),
            &absence_substitution_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| {
                absence_substitution_retries.set(absence_substitution_retries.get() + 1)
            }, sleep: |_| std::future::ready(()) },
            |operation| {
                match &operation {
                    ArtifactOperation::Sync { checkpoint, .. }
                    | ArtifactOperation::Observe(checkpoint) => {
                        absence_substitution_checkpoints
                            .borrow_mut()
                            .push(*checkpoint);
                        if *checkpoint
                            == ArtifactCheckpoint::BeforeAuthoritativePartAbsenceProof
                        {
                            std::fs::write(&absence_part, b"replacement").unwrap();
                            absence_substituted.set(true);
                        }
                    }
                    ArtifactOperation::Write { .. } => {}
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();

        assert!(absence_substituted.get());
        assert_eq!(absence_substitution, DownloadFailure::Durability);
        assert_eq!(absence_substitution_retries.get(), 0);
        assert_eq!(&*absence_substitution_transport.offsets.borrow(), &[None]);
        assert_eq!(
            absence_substitution_checkpoints.borrow().last(),
            Some(&ArtifactCheckpoint::BeforeAuthoritativePartAbsenceProof)
        );
        assert_eq!(std::fs::read(&absence_part).unwrap(), b"replacement");
        assert!(!absence_substitution_dir
            .path()
            .join("model.gguf")
            .exists());
        assert!(!absence_substitution_dir
            .path()
            .join("model.gguf.part.restart")
            .exists());

        let post_proof_dir = tempdir().unwrap();
        let post_proof_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, received)]),
            offsets: RefCell::new(Vec::new()),
        };
        let post_proof_retries = Cell::new(0);
        let post_proof_checkpoints = RefCell::new(Vec::new());
        let post_proof_recreated = Cell::new(false);
        let post_proof_part = post_proof_dir.path().join("model.gguf.part");

        let post_proof = download_with_transport_controlled(
            &spec(expected),
            post_proof_dir.path(),
            &post_proof_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| post_proof_retries.set(post_proof_retries.get() + 1), sleep: |_| std::future::ready(()) },
            |operation| {
                match &operation {
                    ArtifactOperation::Sync { checkpoint, .. }
                    | ArtifactOperation::Observe(checkpoint) => {
                        post_proof_checkpoints.borrow_mut().push(*checkpoint);
                        if *checkpoint == ArtifactCheckpoint::AuthoritativePartAbsent {
                            std::fs::write(&post_proof_part, b"replacement").unwrap();
                            post_proof_recreated.set(true);
                        }
                    }
                    ArtifactOperation::Write { .. } => {}
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();

        assert!(post_proof_recreated.get());
        assert_eq!(post_proof, DownloadFailure::Durability);
        assert_eq!(post_proof_retries.get(), 0);
        assert_eq!(&*post_proof_transport.offsets.borrow(), &[None]);
        assert_eq!(
            post_proof_checkpoints.borrow().last(),
            Some(&ArtifactCheckpoint::AuthoritativePartAbsent)
        );
        assert_eq!(std::fs::read(&post_proof_part).unwrap(), b"replacement");
        assert!(!post_proof_dir.path().join("model.gguf").exists());
        assert!(!post_proof_dir
            .path()
            .join("model.gguf.part.restart")
            .exists());

        let absence_swap_root = tempdir().unwrap();
        let absence_swap_model = absence_swap_root.path().join("model");
        let absence_swap_moved = absence_swap_root.path().join("moved-model");
        let absence_swap_replacement = absence_swap_root.path().join("replacement-model");
        std::fs::create_dir(&absence_swap_model).unwrap();
        let absence_swap_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, received)]),
            offsets: RefCell::new(Vec::new()),
        };
        let absence_swap_retries = Cell::new(0);
        let absence_swap_checkpoints = RefCell::new(Vec::new());
        let absence_swapped = Cell::new(false);
        let absence_restored = Cell::new(false);

        let absence_swap = download_with_transport_controlled(
            &spec(expected),
            &absence_swap_model,
            &absence_swap_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| absence_swap_retries.set(absence_swap_retries.get() + 1), sleep: |_| std::future::ready(()) },
            |operation| {
                match &operation {
                    ArtifactOperation::Sync { checkpoint, .. }
                    | ArtifactOperation::Observe(checkpoint) => {
                        absence_swap_checkpoints.borrow_mut().push(*checkpoint);
                        if *checkpoint
                            == ArtifactCheckpoint::BeforeAuthoritativePartAbsenceProof
                        {
                            std::fs::rename(&absence_swap_model, &absence_swap_moved).unwrap();
                            std::fs::create_dir(&absence_swap_model).unwrap();
                            std::fs::write(
                                absence_swap_model.join("model.gguf.part"),
                                b"replacement",
                            )
                            .unwrap();
                            absence_swapped.set(true);
                        } else if *checkpoint == ArtifactCheckpoint::AuthoritativePartAbsent {
                            std::fs::rename(&absence_swap_model, &absence_swap_replacement).unwrap();
                            std::fs::rename(&absence_swap_moved, &absence_swap_model).unwrap();
                            absence_restored.set(true);
                        }
                    }
                    ArtifactOperation::Write { .. } => {}
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();

        assert!(absence_swapped.get());
        assert!(!absence_restored.get());
        assert_eq!(absence_swap, DownloadFailure::Durability);
        assert_eq!(absence_swap_retries.get(), 0);
        assert_eq!(&*absence_swap_transport.offsets.borrow(), &[None]);
        assert_eq!(
            absence_swap_checkpoints.borrow().last(),
            Some(&ArtifactCheckpoint::BeforeAuthoritativePartAbsenceProof)
        );
        assert_eq!(
            std::fs::read(absence_swap_model.join("model.gguf.part")).unwrap(),
            b"replacement"
        );
        assert!(!absence_swap_moved.join("model.gguf.part").exists());
        assert!(!absence_swap_model.join("model.gguf").exists());
        assert!(!absence_swap_moved.join("model.gguf").exists());

        let sync_failure_dir = tempdir().unwrap();
        let sync_failure_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, received)]),
            offsets: RefCell::new(Vec::new()),
        };
        let sync_failure_retries = Cell::new(0);
        let sync_failure_checkpoints = RefCell::new(Vec::new());

        let sync_failure = download_with_transport_controlled(
            &spec(expected),
            sync_failure_dir.path(),
            &sync_failure_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| sync_failure_retries.set(sync_failure_retries.get() + 1), sleep: |_| std::future::ready(()) },
            |operation| match operation {
                ArtifactOperation::Sync { checkpoint, file } => {
                    sync_failure_checkpoints.borrow_mut().push(checkpoint);
                    if checkpoint == ArtifactCheckpoint::ChecksumCleanupDirectorySynced {
                        return Err(ArtifactOperationFailure::Other);
                    }
                    file.sync_all()
                        .map_err(|_| ArtifactOperationFailure::Other)
                }
                ArtifactOperation::Write { file, bytes } => file
                    .write_all(bytes)
                    .map_err(|_| ArtifactOperationFailure::Other),
                ArtifactOperation::Observe(checkpoint) => {
                    sync_failure_checkpoints.borrow_mut().push(checkpoint);
                    Ok(())
                }
            },
        )
        .unwrap_err();

        assert_eq!(sync_failure, DownloadFailure::Durability);
        assert_eq!(sync_failure_retries.get(), 0);
        assert_eq!(&*sync_failure_transport.offsets.borrow(), &[None]);
        assert_eq!(
            sync_failure_checkpoints.borrow().last(),
            Some(&ArtifactCheckpoint::ChecksumCleanupDirectorySynced)
        );
        assert!(!sync_failure_dir
            .path()
            .join("model.gguf.part")
            .exists());
        assert!(!sync_failure_dir.path().join("model.gguf").exists());
        assert!(!sync_failure_dir
            .path()
            .join("model.gguf.part.restart")
            .exists());
    }

    #[test]
    fn checksum_invalid_restart_is_durably_removed_before_old_part_sync_revalidate_and_restat() {
        let expected = b"ghijkl";
        let received = b"abcdef";
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("model.gguf.part"), b"abc").unwrap();
        std::fs::write(dir.path().join("model.gguf.invalid"), b"keep invalid").unwrap();
        std::fs::write(dir.path().join("foreign.bin"), b"keep foreign").unwrap();
        let before = artifact_snapshot(dir.path());
        let transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, received)]),
            offsets: RefCell::new(Vec::new()),
        };
        let retry_notifications = Cell::new(0);
        let checkpoints = RefCell::new(Vec::new());

        let failure = download_with_transport_controlled(
            &spec(expected),
            dir.path(),
            &transport,
            || false,
            |_| {},
            RetryWait { observer: |_| retry_notifications.set(retry_notifications.get() + 1), sleep: |_| std::future::ready(()) },
            |operation| {
                match &operation {
                    ArtifactOperation::Sync { checkpoint, .. }
                    | ArtifactOperation::Observe(checkpoint) => {
                        checkpoints.borrow_mut().push(*checkpoint)
                    }
                    ArtifactOperation::Write { .. } => {}
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();

        assert_eq!(
            failure,
            DownloadFailure::Integrity {
                retained_bytes: 3,
                authority: IntegrityAuthority::Repair,
            }
        );
        assert_eq!(retry_notifications.get(), 0);
        assert_eq!(&*transport.offsets.borrow(), &[Some(3)]);
        assert_eq!(
            &*checkpoints.borrow(),
            &[
                ArtifactCheckpoint::StagingSynced,
                ArtifactCheckpoint::BeforeRestartUnlink,
                ArtifactCheckpoint::RestartIdentityMatched,
                ArtifactCheckpoint::RestartUnlinked,
                ArtifactCheckpoint::ChecksumCleanupDirectorySynced,
                ArtifactCheckpoint::BeforeRestartAbsenceProof,
                ArtifactCheckpoint::RestartAbsent,
                ArtifactCheckpoint::RecoveredPartSynced,
                ArtifactCheckpoint::BeforeRecoveredPartRestat,
                ArtifactCheckpoint::BeforeIntegrityAuthorityObservation,
            ]
        );
        assert_eq!(artifact_snapshot(dir.path()), before);
        assert!(!dir.path().join("model.gguf").exists());
        assert!(!dir.path().join("model.gguf.part.restart").exists());

        let restart_substitution_dir = tempdir().unwrap();
        std::fs::write(
            restart_substitution_dir.path().join("model.gguf.part"),
            b"abc",
        )
        .unwrap();
        let restart_substitution_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, received)]),
            offsets: RefCell::new(Vec::new()),
        };
        let restart_substitution_retries = Cell::new(0);
        let restart_substitution_checkpoints = RefCell::new(Vec::new());
        let restart_substituted = Cell::new(false);
        let restart = restart_substitution_dir
            .path()
            .join("model.gguf.part.restart");
        let original_restart = restart_substitution_dir.path().join("substituted-restart");

        let restart_substitution = download_with_transport_controlled(
            &spec(expected),
            restart_substitution_dir.path(),
            &restart_substitution_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| restart_substitution_retries.set(restart_substitution_retries.get() + 1), sleep: |_| std::future::ready(()) },
            |operation| {
                match &operation {
                    ArtifactOperation::Sync { checkpoint, .. }
                    | ArtifactOperation::Observe(checkpoint) => {
                        restart_substitution_checkpoints
                            .borrow_mut()
                            .push(*checkpoint);
                        if *checkpoint == ArtifactCheckpoint::RestartIdentityMatched {
                            std::fs::rename(&restart, &original_restart).unwrap();
                            std::fs::write(&restart, b"replacement").unwrap();
                            restart_substituted.set(true);
                        }
                    }
                    ArtifactOperation::Write { .. } => {}
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();

        assert!(restart_substituted.get());
        assert_eq!(restart_substitution, DownloadFailure::Durability);
        assert_eq!(restart_substitution_retries.get(), 0);
        assert_eq!(&*restart_substitution_transport.offsets.borrow(), &[Some(3)]);
        assert_eq!(
            restart_substitution_checkpoints.borrow().last(),
            Some(&ArtifactCheckpoint::RestartIdentityMatched)
        );
        assert_eq!(std::fs::read(&restart).unwrap(), b"replacement");
        assert_eq!(std::fs::read(&original_restart).unwrap(), received);
        assert_eq!(
            std::fs::read(restart_substitution_dir.path().join("model.gguf.part")).unwrap(),
            b"abc"
        );
        assert!(!restart_substitution_dir
            .path()
            .join("model.gguf")
            .exists());

        let stale_part_dir = tempdir().unwrap();
        std::fs::write(stale_part_dir.path().join("model.gguf.part"), b"abc").unwrap();
        let stale_part_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, received)]),
            offsets: RefCell::new(Vec::new()),
        };
        let stale_part_retries = Cell::new(0);
        let stale_part_checkpoints = RefCell::new(Vec::new());
        let stale_part_substituted = Cell::new(false);
        let part = stale_part_dir.path().join("model.gguf.part");
        let original_part = stale_part_dir.path().join("substituted-part");

        let stale_part = download_with_transport_controlled(
            &spec(expected),
            stale_part_dir.path(),
            &stale_part_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| stale_part_retries.set(stale_part_retries.get() + 1), sleep: |_| std::future::ready(()) },
            |operation| {
                match &operation {
                    ArtifactOperation::Sync { checkpoint, .. }
                    | ArtifactOperation::Observe(checkpoint) => {
                        stale_part_checkpoints.borrow_mut().push(*checkpoint);
                        if *checkpoint == ArtifactCheckpoint::BeforeRecoveredPartRestat {
                            std::fs::rename(&part, &original_part).unwrap();
                            std::fs::write(&part, b"replacement").unwrap();
                            stale_part_substituted.set(true);
                        }
                    }
                    ArtifactOperation::Write { .. } => {}
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();

        assert!(stale_part_substituted.get());
        assert_eq!(stale_part, DownloadFailure::Durability);
        assert_eq!(stale_part_retries.get(), 0);
        assert_eq!(&*stale_part_transport.offsets.borrow(), &[Some(3)]);
        assert_eq!(
            stale_part_checkpoints.borrow().last(),
            Some(&ArtifactCheckpoint::BeforeRecoveredPartRestat)
        );
        assert_eq!(std::fs::read(&part).unwrap(), b"replacement");
        assert_eq!(std::fs::read(&original_part).unwrap(), b"abc");
        assert!(!stale_part_dir
            .path()
            .join("model.gguf.part.restart")
            .exists());
        assert!(!stale_part_dir.path().join("model.gguf").exists());

        let transport_substitution_dir = tempdir().unwrap();
        let transport_part = transport_substitution_dir
            .path()
            .join("model.gguf.part");
        let transport_original = transport_substitution_dir.path().join("pre-request-part");
        std::fs::write(&transport_part, b"abc").unwrap();
        let transport_substitution = SubstitutingPartTransport {
            part: transport_part.clone(),
            original: transport_original.clone(),
            replacement: Some(b"xyz".to_vec()),
            response: RefCell::new(Some(transfer(StatusCode::OK, None, received))),
            offsets: RefCell::new(Vec::new()),
            substituted: Cell::new(false),
        };
        let transport_substitution_retries = Cell::new(0);

        let transport_substitution_failure = download_with_transport_controlled(
            &spec(expected),
            transport_substitution_dir.path(),
            &transport_substitution,
            || false,
            |_| {},
            RetryWait { observer: |_| {
                transport_substitution_retries.set(transport_substitution_retries.get() + 1)
            }, sleep: |_| std::future::ready(()) },
            perform_artifact_operation,
        )
        .unwrap_err();

        assert!(transport_substitution.substituted.get());
        assert_eq!(transport_substitution_failure, DownloadFailure::Durability);
        assert_eq!(transport_substitution_retries.get(), 0);
        assert_eq!(&*transport_substitution.offsets.borrow(), &[Some(3)]);
        assert_eq!(std::fs::read(&transport_part).unwrap(), b"xyz");
        assert_eq!(std::fs::read(&transport_original).unwrap(), b"abc");
        assert!(!transport_substitution_dir
            .path()
            .join("model.gguf.part.restart")
            .exists());
        assert!(!transport_substitution_dir
            .path()
            .join("model.gguf")
            .exists());

        let request_swap_root = tempdir().unwrap();
        let request_swap_model = request_swap_root.path().join("model");
        let request_swap_moved = request_swap_root.path().join("moved-model");
        std::fs::create_dir(&request_swap_model).unwrap();
        std::fs::write(request_swap_model.join("model.gguf.part"), b"abc").unwrap();
        std::fs::write(request_swap_model.join("foreign.bin"), b"original foreign").unwrap();
        let request_swap_transport = SubstitutingDirectoryTransport {
            model_dir: request_swap_model.clone(),
            moved_dir: request_swap_moved.clone(),
            response: RefCell::new(Some(transfer(StatusCode::OK, None, received))),
            offsets: RefCell::new(Vec::new()),
            substituted: Cell::new(false),
        };
        let request_swap_retries = Cell::new(0);

        let request_swap = download_with_transport_controlled(
            &spec(expected),
            &request_swap_model,
            &request_swap_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| request_swap_retries.set(request_swap_retries.get() + 1), sleep: |_| std::future::ready(()) },
            perform_artifact_operation,
        )
        .unwrap_err();

        assert!(request_swap_transport.substituted.get());
        assert_eq!(request_swap, DownloadFailure::Durability);
        assert_eq!(request_swap_retries.get(), 0);
        assert_eq!(&*request_swap_transport.offsets.borrow(), &[Some(3)]);
        assert_eq!(
            std::fs::read(request_swap_model.join("foreign.bin")).unwrap(),
            b"replacement foreign"
        );
        assert_eq!(
            std::fs::read(request_swap_moved.join("foreign.bin")).unwrap(),
            b"original foreign"
        );
        assert_eq!(
            std::fs::read(request_swap_moved.join("model.gguf.part")).unwrap(),
            b"abc"
        );
        assert!(!request_swap_model
            .join("model.gguf.part.restart")
            .exists());
        assert!(!request_swap_moved
            .join("model.gguf.part.restart")
            .exists());
        assert!(!request_swap_model.join("model.gguf").exists());
        assert!(!request_swap_moved.join("model.gguf").exists());

        let append_substitution_dir = tempdir().unwrap();
        let append_part = append_substitution_dir.path().join("model.gguf.part");
        let append_original = append_substitution_dir.path().join("pre-request-part");
        std::fs::write(&append_part, b"abc").unwrap();
        let append_substitution = SubstitutingPartTransport {
            part: append_part.clone(),
            original: append_original.clone(),
            replacement: Some(b"xyz".to_vec()),
            response: RefCell::new(Some(transfer(
                StatusCode::PARTIAL_CONTENT,
                Some("bytes 3-5/6"),
                b"def",
            ))),
            offsets: RefCell::new(Vec::new()),
            substituted: Cell::new(false),
        };
        let append_substitution_retries = Cell::new(0);

        let append_substitution_failure = download_with_transport_controlled(
            &spec(b"abcdef"),
            append_substitution_dir.path(),
            &append_substitution,
            || false,
            |_| {},
            RetryWait { observer: |_| append_substitution_retries.set(append_substitution_retries.get() + 1), sleep: |_| std::future::ready(()) },
            perform_artifact_operation,
        )
        .unwrap_err();

        assert!(append_substitution.substituted.get());
        assert_eq!(append_substitution_failure, DownloadFailure::Durability);
        assert_eq!(append_substitution_retries.get(), 0);
        assert_eq!(&*append_substitution.offsets.borrow(), &[Some(3)]);
        assert_eq!(std::fs::read(&append_part).unwrap(), b"xyz");
        assert_eq!(std::fs::read(&append_original).unwrap(), b"abc");
        assert!(!append_substitution_dir
            .path()
            .join("model.gguf.part.restart")
            .exists());
        assert!(!append_substitution_dir
            .path()
            .join("model.gguf")
            .exists());

        for failed_checkpoint in [
            ArtifactCheckpoint::ChecksumCleanupDirectorySynced,
            ArtifactCheckpoint::RecoveredPartSynced,
        ] {
            let sync_failure_dir = tempdir().unwrap();
            std::fs::write(sync_failure_dir.path().join("model.gguf.part"), b"abc").unwrap();
            let sync_failure_transport = FakeTransport {
                responses: RefCell::new(vec![transfer(StatusCode::OK, None, received)]),
                offsets: RefCell::new(Vec::new()),
            };
            let sync_failure_retries = Cell::new(0);
            let sync_failure_checkpoints = RefCell::new(Vec::new());

            let sync_failure = download_with_transport_controlled(
                &spec(expected),
                sync_failure_dir.path(),
                &sync_failure_transport,
                || false,
                |_| {},
                RetryWait { observer: |_| sync_failure_retries.set(sync_failure_retries.get() + 1), sleep: |_| std::future::ready(()) },
                |operation| match operation {
                    ArtifactOperation::Sync { checkpoint, file } => {
                        sync_failure_checkpoints.borrow_mut().push(checkpoint);
                        if checkpoint == failed_checkpoint {
                            return Err(ArtifactOperationFailure::Other);
                        }
                        file.sync_all()
                            .map_err(|_| ArtifactOperationFailure::Other)
                    }
                    ArtifactOperation::Write { file, bytes } => file
                        .write_all(bytes)
                        .map_err(|_| ArtifactOperationFailure::Other),
                    ArtifactOperation::Observe(checkpoint) => {
                        sync_failure_checkpoints.borrow_mut().push(checkpoint);
                        Ok(())
                    }
                },
            )
            .unwrap_err();

            assert_eq!(sync_failure, DownloadFailure::Durability);
            assert_eq!(sync_failure_retries.get(), 0);
            assert_eq!(&*sync_failure_transport.offsets.borrow(), &[Some(3)]);
            assert_eq!(
                sync_failure_checkpoints.borrow().last(),
                Some(&failed_checkpoint)
            );
            assert_eq!(
                std::fs::read(sync_failure_dir.path().join("model.gguf.part")).unwrap(),
                b"abc"
            );
            assert!(!sync_failure_dir
                .path()
                .join("model.gguf.part.restart")
                .exists());
            assert!(!sync_failure_dir.path().join("model.gguf").exists());
        }

        let unlink_swap_root = tempdir().unwrap();
        let unlink_swap_model = unlink_swap_root.path().join("model");
        let unlink_swap_moved = unlink_swap_root.path().join("moved-model");
        std::fs::create_dir(&unlink_swap_model).unwrap();
        std::fs::write(unlink_swap_model.join("model.gguf.part"), b"abc").unwrap();
        let unlink_swap_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, received)]),
            offsets: RefCell::new(Vec::new()),
        };
        let unlink_swap_retries = Cell::new(0);
        let unlink_swap_checkpoints = RefCell::new(Vec::new());
        let unlink_swapped = Cell::new(false);

        let unlink_swap = download_with_transport_controlled(
            &spec(expected),
            &unlink_swap_model,
            &unlink_swap_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| unlink_swap_retries.set(unlink_swap_retries.get() + 1), sleep: |_| std::future::ready(()) },
            |operation| {
                match &operation {
                    ArtifactOperation::Sync { checkpoint, .. }
                    | ArtifactOperation::Observe(checkpoint) => {
                        unlink_swap_checkpoints.borrow_mut().push(*checkpoint);
                        if *checkpoint == ArtifactCheckpoint::RestartIdentityMatched {
                            std::fs::rename(&unlink_swap_model, &unlink_swap_moved).unwrap();
                            std::fs::create_dir(&unlink_swap_model).unwrap();
                            std::fs::write(
                                unlink_swap_model.join("model.gguf.part.restart"),
                                b"replacement",
                            )
                            .unwrap();
                            unlink_swapped.set(true);
                        }
                    }
                    ArtifactOperation::Write { .. } => {}
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();

        assert!(unlink_swapped.get());
        assert_eq!(unlink_swap, DownloadFailure::Durability);
        assert_eq!(unlink_swap_retries.get(), 0);
        assert_eq!(&*unlink_swap_transport.offsets.borrow(), &[Some(3)]);
        assert_eq!(
            unlink_swap_checkpoints.borrow().last(),
            Some(&ArtifactCheckpoint::ChecksumCleanupDirectorySynced)
        );
        assert_eq!(
            std::fs::read(unlink_swap_model.join("model.gguf.part.restart")).unwrap(),
            b"replacement"
        );
        assert!(!unlink_swap_moved
            .join("model.gguf.part.restart")
            .exists());
        assert_eq!(
            std::fs::read(unlink_swap_moved.join("model.gguf.part")).unwrap(),
            b"abc"
        );

        let absence_recreation_dir = tempdir().unwrap();
        std::fs::write(
            absence_recreation_dir.path().join("model.gguf.part"),
            b"abc",
        )
        .unwrap();
        let absence_recreation_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, received)]),
            offsets: RefCell::new(Vec::new()),
        };
        let absence_recreation_retries = Cell::new(0);
        let absence_recreation_checkpoints = RefCell::new(Vec::new());
        let restart_recreated = Cell::new(false);
        let recreated_restart = absence_recreation_dir
            .path()
            .join("model.gguf.part.restart");

        let absence_recreation = download_with_transport_controlled(
            &spec(expected),
            absence_recreation_dir.path(),
            &absence_recreation_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| absence_recreation_retries.set(absence_recreation_retries.get() + 1), sleep: |_| std::future::ready(()) },
            |operation| {
                match &operation {
                    ArtifactOperation::Sync { checkpoint, .. }
                    | ArtifactOperation::Observe(checkpoint) => {
                        absence_recreation_checkpoints
                            .borrow_mut()
                            .push(*checkpoint);
                        if *checkpoint == ArtifactCheckpoint::RestartAbsent {
                            std::fs::write(&recreated_restart, b"replacement").unwrap();
                            restart_recreated.set(true);
                        }
                    }
                    ArtifactOperation::Write { .. } => {}
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();

        assert!(restart_recreated.get());
        assert_eq!(absence_recreation, DownloadFailure::Durability);
        assert_eq!(absence_recreation_retries.get(), 0);
        assert_eq!(&*absence_recreation_transport.offsets.borrow(), &[Some(3)]);
        assert_eq!(
            absence_recreation_checkpoints.borrow().last(),
            Some(&ArtifactCheckpoint::RestartAbsent)
        );
        assert_eq!(std::fs::read(&recreated_restart).unwrap(), b"replacement");
        assert_eq!(
            std::fs::read(absence_recreation_dir.path().join("model.gguf.part")).unwrap(),
            b"abc"
        );

        let absence_swap_root = tempdir().unwrap();
        let absence_swap_model = absence_swap_root.path().join("model");
        let absence_swap_moved = absence_swap_root.path().join("moved-model");
        let absence_swap_replacement = absence_swap_root.path().join("replacement-model");
        std::fs::create_dir(&absence_swap_model).unwrap();
        std::fs::write(absence_swap_model.join("model.gguf.part"), b"abc").unwrap();
        let absence_swap_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, received)]),
            offsets: RefCell::new(Vec::new()),
        };
        let absence_swap_retries = Cell::new(0);
        let absence_swap_checkpoints = RefCell::new(Vec::new());
        let absence_swapped = Cell::new(false);
        let absence_restored = Cell::new(false);

        let absence_swap = download_with_transport_controlled(
            &spec(expected),
            &absence_swap_model,
            &absence_swap_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| absence_swap_retries.set(absence_swap_retries.get() + 1), sleep: |_| std::future::ready(()) },
            |operation| {
                match &operation {
                    ArtifactOperation::Sync { checkpoint, .. }
                    | ArtifactOperation::Observe(checkpoint) => {
                        absence_swap_checkpoints.borrow_mut().push(*checkpoint);
                        if *checkpoint == ArtifactCheckpoint::BeforeRestartAbsenceProof {
                            std::fs::rename(&absence_swap_model, &absence_swap_moved).unwrap();
                            std::fs::create_dir(&absence_swap_model).unwrap();
                            std::fs::write(
                                absence_swap_model.join("model.gguf.part.restart"),
                                b"replacement",
                            )
                            .unwrap();
                            absence_swapped.set(true);
                        } else if *checkpoint == ArtifactCheckpoint::RestartAbsent {
                            std::fs::rename(&absence_swap_model, &absence_swap_replacement).unwrap();
                            std::fs::rename(&absence_swap_moved, &absence_swap_model).unwrap();
                            absence_restored.set(true);
                        }
                    }
                    ArtifactOperation::Write { .. } => {}
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();

        assert!(absence_swapped.get());
        assert!(!absence_restored.get());
        assert_eq!(absence_swap, DownloadFailure::Durability);
        assert_eq!(absence_swap_retries.get(), 0);
        assert_eq!(&*absence_swap_transport.offsets.borrow(), &[Some(3)]);
        assert_eq!(
            absence_swap_checkpoints.borrow().last(),
            Some(&ArtifactCheckpoint::BeforeRestartAbsenceProof)
        );
        assert_eq!(
            std::fs::read(absence_swap_model.join("model.gguf.part.restart")).unwrap(),
            b"replacement"
        );
        assert!(!absence_swap_moved
            .join("model.gguf.part.restart")
            .exists());
        assert_eq!(
            std::fs::read(absence_swap_moved.join("model.gguf.part")).unwrap(),
            b"abc"
        );
    }

    #[test]
    fn checksum_cleanup_or_directory_sync_failure_is_durability_without_recovery() {
        let expected = b"ghijkl";
        let received = b"abcdef";

        for failed_checkpoint in [
            ArtifactCheckpoint::BeforeAuthoritativePartUnlink,
            ArtifactCheckpoint::AuthoritativePartIdentityMatched,
            ArtifactCheckpoint::AuthoritativePartUnlinked,
            ArtifactCheckpoint::ChecksumCleanupDirectorySynced,
            ArtifactCheckpoint::BeforeAuthoritativePartAbsenceProof,
            ArtifactCheckpoint::AuthoritativePartAbsent,
            ArtifactCheckpoint::BeforeIntegrityAuthorityObservation,
        ] {
            let dir = tempdir().unwrap();
            std::fs::write(dir.path().join("model.gguf.part.restart"), b"keep restart").unwrap();
            std::fs::write(dir.path().join("model.gguf.invalid"), b"keep invalid").unwrap();
            std::fs::write(dir.path().join("foreign.bin"), b"keep foreign").unwrap();
            let transport = FakeTransport {
                responses: RefCell::new(vec![transfer(StatusCode::OK, None, received)]),
                offsets: RefCell::new(Vec::new()),
            };
            let retries = Cell::new(0);
            let checkpoints = RefCell::new(Vec::new());

            let failure = download_with_transport_controlled(
                &spec(expected),
                dir.path(),
                &transport,
                || false,
                |_| {},
                RetryWait { observer: |_| retries.set(retries.get() + 1), sleep: |_| std::future::ready(()) },
                |operation| {
                    let checkpoint = match &operation {
                        ArtifactOperation::Sync { checkpoint, .. }
                        | ArtifactOperation::Observe(checkpoint) => Some(*checkpoint),
                        ArtifactOperation::Write { .. } => None,
                    };
                    if let Some(checkpoint) = checkpoint {
                        checkpoints.borrow_mut().push(checkpoint);
                        if checkpoint == failed_checkpoint {
                            return Err(ArtifactOperationFailure::Other);
                        }
                    }
                    perform_artifact_operation(operation)
                },
            )
            .unwrap_err();

            assert_eq!(failure, DownloadFailure::Durability);
            assert_eq!(retries.get(), 0);
            assert_eq!(&*transport.offsets.borrow(), &[None]);
            assert_eq!(checkpoints.borrow().last(), Some(&failed_checkpoint));
            assert_eq!(
                std::fs::read(dir.path().join("model.gguf.part.restart")).unwrap(),
                b"keep restart"
            );
            assert_eq!(
                std::fs::read(dir.path().join("model.gguf.invalid")).unwrap(),
                b"keep invalid"
            );
            assert_eq!(
                std::fs::read(dir.path().join("foreign.bin")).unwrap(),
                b"keep foreign"
            );
            assert!(!dir.path().join("model.gguf").exists());
        }

        for failed_checkpoint in [
            ArtifactCheckpoint::BeforeRestartUnlink,
            ArtifactCheckpoint::RestartIdentityMatched,
            ArtifactCheckpoint::RestartUnlinked,
            ArtifactCheckpoint::ChecksumCleanupDirectorySynced,
            ArtifactCheckpoint::BeforeRestartAbsenceProof,
            ArtifactCheckpoint::RestartAbsent,
            ArtifactCheckpoint::RecoveredPartSynced,
            ArtifactCheckpoint::BeforeRecoveredPartRestat,
            ArtifactCheckpoint::BeforeIntegrityAuthorityObservation,
        ] {
            let dir = tempdir().unwrap();
            std::fs::write(dir.path().join("model.gguf.part"), b"abc").unwrap();
            std::fs::write(dir.path().join("model.gguf.invalid"), b"keep invalid").unwrap();
            std::fs::write(dir.path().join("foreign.bin"), b"keep foreign").unwrap();
            let transport = FakeTransport {
                responses: RefCell::new(vec![transfer(StatusCode::OK, None, received)]),
                offsets: RefCell::new(Vec::new()),
            };
            let retries = Cell::new(0);
            let checkpoints = RefCell::new(Vec::new());

            let failure = download_with_transport_controlled(
                &spec(expected),
                dir.path(),
                &transport,
                || false,
                |_| {},
                RetryWait { observer: |_| retries.set(retries.get() + 1), sleep: |_| std::future::ready(()) },
                |operation| {
                    let checkpoint = match &operation {
                        ArtifactOperation::Sync { checkpoint, .. }
                        | ArtifactOperation::Observe(checkpoint) => Some(*checkpoint),
                        ArtifactOperation::Write { .. } => None,
                    };
                    if let Some(checkpoint) = checkpoint {
                        checkpoints.borrow_mut().push(checkpoint);
                        if checkpoint == failed_checkpoint {
                            return Err(ArtifactOperationFailure::Other);
                        }
                    }
                    perform_artifact_operation(operation)
                },
            )
            .unwrap_err();

            assert_eq!(failure, DownloadFailure::Durability);
            assert_eq!(retries.get(), 0);
            assert_eq!(&*transport.offsets.borrow(), &[Some(3)]);
            assert_eq!(checkpoints.borrow().last(), Some(&failed_checkpoint));
            assert_eq!(
                std::fs::read(dir.path().join("model.gguf.part")).unwrap(),
                b"abc"
            );
            assert_eq!(
                std::fs::read(dir.path().join("model.gguf.invalid")).unwrap(),
                b"keep invalid"
            );
            assert_eq!(
                std::fs::read(dir.path().join("foreign.bin")).unwrap(),
                b"keep foreign"
            );
            assert!(!dir.path().join("model.gguf").exists());
        }
    }

    #[test]
    fn installed_or_invalid_integrity_repair_outcome_is_non_discardable() {
        let expected = b"ghijkl";
        let received = b"abcdef";

        let installed_dir = tempdir().unwrap();
        let installed_path = installed_dir.path().join("model.gguf");
        std::fs::write(&installed_path, expected).unwrap();
        let installed_transport = FakeTransport {
            responses: RefCell::new(Vec::new()),
            offsets: RefCell::new(Vec::new()),
        };

        let installed = download_with_transport_controlled(
            &spec(expected),
            installed_dir.path(),
            &installed_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| panic!("an exact installed artifact must not retry"), sleep: |_| std::future::ready(()) },
            perform_artifact_operation,
        )
        .unwrap();

        assert_eq!(
            installed,
            DownloadTerminalOutcome::Complete(DownloadOutcome::AlreadyInstalled(installed_path))
        );
        assert!(installed_transport.offsets.borrow().is_empty());

        let pending_only_dir = tempdir().unwrap();
        let pending_only_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, received)]),
            offsets: RefCell::new(Vec::new()),
        };

        let pending_only = download_with_transport_controlled(
            &spec(expected),
            pending_only_dir.path(),
            &pending_only_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| panic!("integrity failure must not retry"), sleep: |_| std::future::ready(()) },
            perform_artifact_operation,
        )
        .unwrap_err();

        assert_eq!(
            pending_only,
            DownloadFailure::Integrity {
                retained_bytes: 0,
                authority: IntegrityAuthority::PendingOnly,
            }
        );

        let invalid_part_dir = tempdir().unwrap();
        std::fs::write(
            invalid_part_dir.path().join("model.gguf.invalid"),
            b"repair evidence",
        )
        .unwrap();
        let invalid_part_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, received)]),
            offsets: RefCell::new(Vec::new()),
        };

        let invalid_part = download_with_transport_controlled(
            &spec(expected),
            invalid_part_dir.path(),
            &invalid_part_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| panic!("integrity failure must not retry"), sleep: |_| std::future::ready(()) },
            perform_artifact_operation,
        )
        .unwrap_err();

        assert_eq!(
            invalid_part,
            DownloadFailure::Integrity {
                retained_bytes: 0,
                authority: IntegrityAuthority::Repair,
            }
        );
        assert_eq!(
            std::fs::read(invalid_part_dir.path().join("model.gguf.invalid")).unwrap(),
            b"repair evidence"
        );

        let invalid_restart_dir = tempdir().unwrap();
        std::fs::write(
            invalid_restart_dir.path().join("model.gguf.invalid"),
            b"repair evidence",
        )
        .unwrap();
        std::fs::write(
            invalid_restart_dir.path().join("model.gguf.part"),
            b"abc",
        )
        .unwrap();
        let invalid_restart_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, received)]),
            offsets: RefCell::new(Vec::new()),
        };

        let invalid_restart = download_with_transport_controlled(
            &spec(expected),
            invalid_restart_dir.path(),
            &invalid_restart_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| panic!("integrity failure must not retry"), sleep: |_| std::future::ready(()) },
            perform_artifact_operation,
        )
        .unwrap_err();

        assert_eq!(
            invalid_restart,
            DownloadFailure::Integrity {
                retained_bytes: 3,
                authority: IntegrityAuthority::Repair,
            }
        );
        assert_eq!(
            std::fs::read(invalid_restart_dir.path().join("model.gguf.invalid")).unwrap(),
            b"repair evidence"
        );

        let renamed_final_dir = tempdir().unwrap();
        std::fs::write(renamed_final_dir.path().join("model.gguf"), b"xxxxxx").unwrap();
        let renamed_final_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, received)]),
            offsets: RefCell::new(Vec::new()),
        };

        let renamed_final = download_with_transport_controlled(
            &spec(expected),
            renamed_final_dir.path(),
            &renamed_final_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| panic!("integrity failure must not retry"), sleep: |_| std::future::ready(()) },
            perform_artifact_operation,
        )
        .unwrap_err();

        assert_eq!(
            renamed_final,
            DownloadFailure::Integrity {
                retained_bytes: 0,
                authority: IntegrityAuthority::Repair,
            }
        );
        assert_eq!(
            std::fs::read(renamed_final_dir.path().join("model.gguf.invalid")).unwrap(),
            b"xxxxxx"
        );

        let removed_invalid_dir = tempdir().unwrap();
        let removed_invalid_path = removed_invalid_dir.path().join("model.gguf.invalid");
        std::fs::write(&removed_invalid_path, b"repair evidence").unwrap();
        let removed_invalid_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, received)]),
            offsets: RefCell::new(Vec::new()),
        };
        let removed_invalid = Cell::new(false);

        let removed_invalid_failure = download_with_transport_controlled(
            &spec(expected),
            removed_invalid_dir.path(),
            &removed_invalid_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| panic!("integrity failure must not retry"), sleep: |_| std::future::ready(()) },
            |operation| {
                if matches!(&operation, ArtifactOperation::Write { .. })
                    && !removed_invalid.replace(true)
                {
                    std::fs::remove_file(&removed_invalid_path).unwrap();
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();

        assert!(removed_invalid.get());
        assert_eq!(
            removed_invalid_failure,
            DownloadFailure::Integrity {
                retained_bytes: 0,
                authority: IntegrityAuthority::Repair,
            }
        );

        let removed_renamed_dir = tempdir().unwrap();
        let removed_renamed_path = removed_renamed_dir.path().join("model.gguf.invalid");
        std::fs::write(removed_renamed_dir.path().join("model.gguf"), b"xxxxxx").unwrap();
        let removed_renamed_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, received)]),
            offsets: RefCell::new(Vec::new()),
        };
        let removed_renamed = Cell::new(false);

        let removed_renamed_failure = download_with_transport_controlled(
            &spec(expected),
            removed_renamed_dir.path(),
            &removed_renamed_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| panic!("integrity failure must not retry"), sleep: |_| std::future::ready(()) },
            |operation| {
                if matches!(&operation, ArtifactOperation::Write { .. })
                    && !removed_renamed.replace(true)
                {
                    std::fs::remove_file(&removed_renamed_path).unwrap();
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();

        assert!(removed_renamed.get());
        assert_eq!(
            removed_renamed_failure,
            DownloadFailure::Integrity {
                retained_bytes: 0,
                authority: IntegrityAuthority::Repair,
            }
        );

        let late_invalid_dir = tempdir().unwrap();
        let late_invalid_path = late_invalid_dir.path().join("model.gguf.invalid");
        let late_invalid_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, received)]),
            offsets: RefCell::new(Vec::new()),
        };
        let late_invalid_created = Cell::new(false);

        let late_invalid = download_with_transport_controlled(
            &spec(expected),
            late_invalid_dir.path(),
            &late_invalid_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| panic!("integrity failure must not retry"), sleep: |_| std::future::ready(()) },
            |operation| {
                if matches!(&operation, ArtifactOperation::Write { .. })
                    && !late_invalid_created.replace(true)
                {
                    std::fs::write(&late_invalid_path, b"late repair evidence").unwrap();
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();

        assert!(late_invalid_created.get());
        assert_eq!(
            late_invalid,
            DownloadFailure::Integrity {
                retained_bytes: 0,
                authority: IntegrityAuthority::Repair,
            }
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let unsafe_invalid_dir = tempdir().unwrap();
            let unsafe_invalid_path = unsafe_invalid_dir.path().join("model.gguf.invalid");
            let outside = unsafe_invalid_dir.path().join("outside");
            std::fs::write(&outside, b"outside witness").unwrap();
            let unsafe_invalid_transport = FakeTransport {
                responses: RefCell::new(vec![transfer(StatusCode::OK, None, received)]),
                offsets: RefCell::new(Vec::new()),
            };
            let unsafe_invalid_created = Cell::new(false);

            let unsafe_invalid = download_with_transport_controlled(
                &spec(expected),
                unsafe_invalid_dir.path(),
                &unsafe_invalid_transport,
                || false,
                |_| {},
                RetryWait { observer: |_| panic!("integrity failure must not retry"), sleep: |_| std::future::ready(()) },
                |operation| {
                    if matches!(&operation, ArtifactOperation::Write { .. })
                        && !unsafe_invalid_created.replace(true)
                    {
                        symlink(&outside, &unsafe_invalid_path).unwrap();
                    }
                    perform_artifact_operation(operation)
                },
            )
            .unwrap_err();

            assert!(unsafe_invalid_created.get());
            assert_eq!(unsafe_invalid, DownloadFailure::Durability);
            assert_eq!(std::fs::read(&outside).unwrap(), b"outside witness");
            assert!(std::fs::symlink_metadata(&unsafe_invalid_path)
                .unwrap()
                .file_type()
                .is_symlink());
        }

        let authority_hook_dir = tempdir().unwrap();
        let authority_hook_invalid = authority_hook_dir.path().join("model.gguf.invalid");
        let authority_hook_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, received)]),
            offsets: RefCell::new(Vec::new()),
        };
        let authority_hook_created = Cell::new(false);

        let authority_hook = download_with_transport_controlled(
            &spec(expected),
            authority_hook_dir.path(),
            &authority_hook_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| panic!("integrity failure must not retry"), sleep: |_| std::future::ready(()) },
            |operation| {
                if matches!(
                    &operation,
                    ArtifactOperation::Observe(
                        ArtifactCheckpoint::BeforeIntegrityAuthorityObservation
                    )
                ) && !authority_hook_created.replace(true)
                {
                    std::fs::write(&authority_hook_invalid, b"hook repair evidence").unwrap();
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();

        assert!(authority_hook_created.get());
        assert_eq!(
            authority_hook,
            DownloadFailure::Integrity {
                retained_bytes: 0,
                authority: IntegrityAuthority::Repair,
            }
        );
        assert_eq!(
            std::fs::read(&authority_hook_invalid).unwrap(),
            b"hook repair evidence"
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let unsafe_hook_dir = tempdir().unwrap();
            let unsafe_hook_invalid = unsafe_hook_dir.path().join("model.gguf.invalid");
            let unsafe_hook_outside = unsafe_hook_dir.path().join("outside");
            std::fs::write(&unsafe_hook_outside, b"outside hook witness").unwrap();
            let unsafe_hook_transport = FakeTransport {
                responses: RefCell::new(vec![transfer(StatusCode::OK, None, received)]),
                offsets: RefCell::new(Vec::new()),
            };
            let unsafe_hook_created = Cell::new(false);

            let unsafe_hook = download_with_transport_controlled(
                &spec(expected),
                unsafe_hook_dir.path(),
                &unsafe_hook_transport,
                || false,
                |_| {},
                RetryWait { observer: |_| panic!("integrity failure must not retry"), sleep: |_| std::future::ready(()) },
                |operation| {
                    if matches!(
                        &operation,
                        ArtifactOperation::Observe(
                            ArtifactCheckpoint::BeforeIntegrityAuthorityObservation
                        )
                    ) && !unsafe_hook_created.replace(true)
                    {
                        symlink(&unsafe_hook_outside, &unsafe_hook_invalid).unwrap();
                    }
                    perform_artifact_operation(operation)
                },
            )
            .unwrap_err();

            assert!(unsafe_hook_created.get());
            assert_eq!(unsafe_hook, DownloadFailure::Durability);
            assert_eq!(
                std::fs::read(&unsafe_hook_outside).unwrap(),
                b"outside hook witness"
            );
            assert!(std::fs::symlink_metadata(&unsafe_hook_invalid)
                .unwrap()
                .file_type()
                .is_symlink());
        }

        let authority_swap_root = tempdir().unwrap();
        let authority_swap_model = authority_swap_root.path().join("model");
        let authority_swap_moved = authority_swap_root.path().join("moved-model");
        std::fs::create_dir(&authority_swap_model).unwrap();
        std::fs::write(
            authority_swap_model.join("foreign.bin"),
            b"original foreign",
        )
        .unwrap();
        let authority_swap_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, received)]),
            offsets: RefCell::new(Vec::new()),
        };
        let authority_swapped = Cell::new(false);

        let authority_swap = download_with_transport_controlled(
            &spec(expected),
            &authority_swap_model,
            &authority_swap_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| panic!("integrity failure must not retry"), sleep: |_| std::future::ready(()) },
            |operation| {
                if matches!(
                    &operation,
                    ArtifactOperation::Observe(
                        ArtifactCheckpoint::BeforeIntegrityAuthorityObservation
                    )
                ) && !authority_swapped.replace(true)
                {
                    std::fs::rename(&authority_swap_model, &authority_swap_moved).unwrap();
                    std::fs::create_dir(&authority_swap_model).unwrap();
                    std::fs::write(
                        authority_swap_model.join("foreign.bin"),
                        b"replacement foreign",
                    )
                    .unwrap();
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();

        assert!(authority_swapped.get());
        assert_eq!(authority_swap, DownloadFailure::Durability);
        assert_eq!(&*authority_swap_transport.offsets.borrow(), &[None]);
        assert_eq!(
            std::fs::read(authority_swap_model.join("foreign.bin")).unwrap(),
            b"replacement foreign"
        );
        assert_eq!(
            std::fs::read(authority_swap_moved.join("foreign.bin")).unwrap(),
            b"original foreign"
        );
        assert!(!authority_swap_model.join("model.gguf").exists());
        assert!(!authority_swap_moved.join("model.gguf").exists());
    }

    #[test]
    fn progress_is_monotonic_capped_at_total_and_neutral_across_resume_restart_and_verify() {
        let bytes = b"abcdef";
        let transferring = |transferred| ProgressUpdate::Transferring {
            transferred,
            total: bytes.len() as u64,
        };
        let verifying = ProgressUpdate::Verifying {
            transferred: bytes.len() as u64,
            total: bytes.len() as u64,
        };

        let fresh_dir = tempdir().unwrap();
        let fresh_transport = FakeTransport {
            responses: RefCell::new(vec![Transfer::test(
                StatusCode::OK,
                None,
                [Ok(b"ab".to_vec()), Ok(b"cdef".to_vec())],
            )]),
            offsets: RefCell::new(Vec::new()),
        };
        let mut fresh = Vec::new();
        download_with_transport_progress(&spec(bytes), fresh_dir.path(), &fresh_transport, |event| {
            fresh.push(event)
        })
        .unwrap();
        assert_eq!(
            fresh,
            [transferring(0), transferring(2), transferring(6), verifying]
        );

        let resume_dir = tempdir().unwrap();
        std::fs::write(resume_dir.path().join("model.gguf.part"), b"abc").unwrap();
        let resume_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(
                StatusCode::PARTIAL_CONTENT,
                Some("bytes 3-5/6"),
                b"def",
            )]),
            offsets: RefCell::new(Vec::new()),
        };
        let mut resume = Vec::new();
        download_with_transport_progress(
            &spec(bytes),
            resume_dir.path(),
            &resume_transport,
            |event| resume.push(event),
        )
        .unwrap();
        assert_eq!(resume, [transferring(3), transferring(6), verifying]);

        let restart_dir = tempdir().unwrap();
        std::fs::write(restart_dir.path().join("model.gguf.part"), b"old").unwrap();
        let restart_transport = FakeTransport {
            responses: RefCell::new(vec![Transfer::test(
                StatusCode::OK,
                None,
                [
                    Ok(b"a".to_vec()),
                    Ok(b"bcd".to_vec()),
                    Ok(b"ef".to_vec()),
                ],
            )]),
            offsets: RefCell::new(Vec::new()),
        };
        let mut restart = Vec::new();
        download_with_transport_progress(
            &spec(bytes),
            restart_dir.path(),
            &restart_transport,
            |event| restart.push(event),
        )
        .unwrap();
        assert_eq!(
            restart,
            [transferring(3), transferring(4), transferring(6), verifying]
        );

        let retry_dir = tempdir().unwrap();
        let retry_transport = FakeTransport {
            responses: RefCell::new(vec![
                failing_transfer(StatusCode::OK, None, b"abc"),
                transfer(
                    StatusCode::PARTIAL_CONTENT,
                    Some("bytes 3-5/6"),
                    b"def",
                ),
            ]),
            offsets: RefCell::new(Vec::new()),
        };
        let mut retry = Vec::new();
        download_with_transport_controlled(
            &spec(bytes),
            retry_dir.path(),
            &retry_transport,
            || false,
            |event| retry.push(event),
            RetryWait { observer: |_| {}, sleep: |_| std::future::ready(()) },
            perform_artifact_operation,
        )
        .unwrap();
        assert_eq!(
            retry,
            [transferring(0), transferring(3), transferring(6), verifying]
        );

        let overrun_dir = tempdir().unwrap();
        let overrun_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, b"abcdefg")]),
            offsets: RefCell::new(Vec::new()),
        };
        let mut overrun = Vec::new();
        download_with_transport_progress(
            &spec(bytes),
            overrun_dir.path(),
            &overrun_transport,
            |event| overrun.push(event),
        )
        .unwrap_err();
        assert_eq!(overrun, [transferring(0), transferring(6)]);

        let final_dir = tempdir().unwrap();
        std::fs::write(final_dir.path().join("model.gguf"), bytes).unwrap();
        let final_transport = FakeTransport {
            responses: RefCell::new(Vec::new()),
            offsets: RefCell::new(Vec::new()),
        };
        let mut final_events = Vec::new();
        download_with_transport_progress(&spec(bytes), final_dir.path(), &final_transport, |event| {
            final_events.push(event)
        })
        .unwrap();
        assert_eq!(final_events, [verifying]);

        let corrupt_final_dir = tempdir().unwrap();
        std::fs::write(corrupt_final_dir.path().join("model.gguf"), b"xxxxxx").unwrap();
        let corrupt_final_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, bytes)]),
            offsets: RefCell::new(Vec::new()),
        };
        let mut corrupt_final = Vec::new();
        download_with_transport_progress(
            &spec(bytes),
            corrupt_final_dir.path(),
            &corrupt_final_transport,
            |event| corrupt_final.push(event),
        )
        .unwrap();
        assert_eq!(
            corrupt_final,
            [transferring(0), transferring(6), verifying]
        );

        for events in [fresh, resume, restart, retry, overrun, final_events, corrupt_final] {
            let mut last = 0;
            for event in events {
                let (transferred, total) = match event {
                    ProgressUpdate::Transferring { transferred, total }
                    | ProgressUpdate::Verifying { transferred, total } => (transferred, total),
                };
                assert_eq!(total, bytes.len() as u64);
                assert!(transferred >= last);
                assert!(transferred <= total);
                last = transferred;
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn zero_length_existing_part_is_identity_bound_before_zero_offset_write() {
        use std::os::unix::fs::{FileTypeExt, MetadataExt};

        let bytes = b"abcdef";

        let unchanged_dir = tempdir().unwrap();
        std::fs::write(unchanged_dir.path().join("model.gguf.part"), b"").unwrap();
        let unchanged_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, bytes)]),
            offsets: RefCell::new(Vec::new()),
        };
        let unchanged =
            download_with_transport(&spec(bytes), unchanged_dir.path(), &unchanged_transport)
                .unwrap();
        assert_eq!(std::fs::read(unchanged).unwrap(), bytes);
        assert_eq!(&*unchanged_transport.offsets.borrow(), &[None]);

        let dir = tempdir().unwrap();
        let part = dir.path().join("model.gguf.part");
        let original = dir.path().join("pre-request-empty-part");
        std::fs::write(&part, b"").unwrap();
        let original_inode = std::fs::metadata(&part).unwrap().ino();
        let body_polls = std::rc::Rc::new(Cell::new(0));
        let body_polls_for_future = std::rc::Rc::clone(&body_polls);
        let transport = SubstitutingPartTransport {
            part: part.clone(),
            original: original.clone(),
            replacement: Some(Vec::new()),
            response: RefCell::new(Some(Transfer::test_future(
                StatusCode::OK,
                None,
                std::future::poll_fn(move |_| {
                    body_polls_for_future.set(body_polls_for_future.get() + 1);
                    std::task::Poll::Ready(Ok(Some(bytes.to_vec())))
                }),
            ))),
            offsets: RefCell::new(Vec::new()),
            substituted: Cell::new(false),
        };
        let retries = Cell::new(0);

        let failure = download_with_transport_controlled(
            &spec(bytes),
            dir.path(),
            &transport,
            || false,
            |_| {},
            RetryWait { observer: |_| retries.set(retries.get() + 1), sleep: |_| std::future::ready(()) },
            perform_artifact_operation,
        )
        .unwrap_err();

        assert!(transport.substituted.get());
        assert_eq!(failure, DownloadFailure::Durability);
        assert_eq!(retries.get(), 0);
        assert_eq!(&*transport.offsets.borrow(), &[None]);
        assert_eq!(body_polls.get(), 0);
        assert_eq!(std::fs::read(&original).unwrap(), b"");
        assert_eq!(std::fs::metadata(&original).unwrap().ino(), original_inode);
        assert_eq!(std::fs::read(&part).unwrap(), b"");
        assert_ne!(std::fs::metadata(&part).unwrap().ino(), original_inode);
        assert!(!dir.path().join("model.gguf").exists());
        assert!(!dir.path().join("model.gguf.part.restart").exists());

        let absent_dir = tempdir().unwrap();
        let absent_part = absent_dir.path().join("model.gguf.part");
        let absent_original = absent_dir.path().join("pre-request-empty-part");
        std::fs::write(&absent_part, b"").unwrap();
        let absent_inode = std::fs::metadata(&absent_part).unwrap().ino();
        let absent_body_polls = std::rc::Rc::new(Cell::new(0));
        let absent_body_polls_for_future = std::rc::Rc::clone(&absent_body_polls);
        let absent_transport = SubstitutingPartTransport {
            part: absent_part.clone(),
            original: absent_original.clone(),
            replacement: None,
            response: RefCell::new(Some(Transfer::test_future(
                StatusCode::OK,
                None,
                std::future::poll_fn(move |_| {
                    absent_body_polls_for_future.set(absent_body_polls_for_future.get() + 1);
                    std::task::Poll::Ready(Ok(Some(bytes.to_vec())))
                }),
            ))),
            offsets: RefCell::new(Vec::new()),
            substituted: Cell::new(false),
        };
        let absent_retries = Cell::new(0);

        let absent_failure = download_with_transport_controlled(
            &spec(bytes),
            absent_dir.path(),
            &absent_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| absent_retries.set(absent_retries.get() + 1), sleep: |_| std::future::ready(()) },
            perform_artifact_operation,
        )
        .unwrap_err();

        assert!(absent_transport.substituted.get());
        assert_eq!(absent_failure, DownloadFailure::Durability);
        assert_eq!(absent_retries.get(), 0);
        assert_eq!(&*absent_transport.offsets.borrow(), &[None]);
        assert_eq!(absent_body_polls.get(), 0);
        assert_eq!(std::fs::read(&absent_original).unwrap(), b"");
        assert_eq!(std::fs::metadata(&absent_original).unwrap().ino(), absent_inode);
        assert!(!absent_part.exists());
        assert!(!absent_dir.path().join("model.gguf").exists());
        assert!(!absent_dir.path().join("model.gguf.part.restart").exists());

        let fifo_dir = tempdir().unwrap();
        let fifo_part = fifo_dir.path().join("model.gguf.part");
        let fifo_original = fifo_dir.path().join("pre-request-empty-part");
        std::fs::write(&fifo_part, b"").unwrap();
        let fifo_original_inode = std::fs::metadata(&fifo_part).unwrap().ino();
        let fifo_body_polls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let fifo_body_polls_for_future = std::sync::Arc::clone(&fifo_body_polls);
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let worker_dir = fifo_dir.path().to_path_buf();
        let worker_part = fifo_part.clone();
        let worker_original = fifo_original.clone();
        let worker = std::thread::spawn(move || {
            let transport = FifoSubstitutingPartTransport {
                part: worker_part,
                original: worker_original,
                response: RefCell::new(Some(Transfer::test_future(
                    StatusCode::OK,
                    None,
                    std::future::poll_fn(move |_| {
                        fifo_body_polls_for_future.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        std::task::Poll::Ready(Ok(Some(bytes.to_vec())))
                    }),
                ))),
                offsets: RefCell::new(Vec::new()),
                substituted: Cell::new(false),
                started: std::sync::Mutex::new(Some(started_tx)),
            };
            let retries = Cell::new(0);
            let failure = download_with_transport_controlled(
                &spec(bytes),
                &worker_dir,
                &transport,
                || false,
                |_| {},
                RetryWait { observer: |_| retries.set(retries.get() + 1), sleep: |_| std::future::ready(()) },
                perform_artifact_operation,
            )
            .unwrap_err();
            let _ = result_tx.send((
                failure,
                retries.get(),
                transport.offsets.into_inner(),
                transport.substituted.get(),
            ));
        });

        started_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("FIFO substitution did not reach its bounded handshake");
        let first_result = result_rx.recv_timeout(std::time::Duration::from_millis(250));
        let (returned_without_release, fifo_result) = match first_result {
            Ok(result) => (true, result),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                use std::os::unix::fs::OpenOptionsExt;

                let release = std::fs::OpenOptions::new()
                    .read(true)
                    .custom_flags(libc::O_NONBLOCK)
                    .open(&fifo_part)
                    .expect("failed to release blocked FIFO open");
                let result = result_rx
                    .recv_timeout(std::time::Duration::from_secs(2))
                    .expect("FIFO worker did not exit after controlled release");
                drop(release);
                (false, result)
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                worker.join().expect("FIFO worker panicked");
                panic!("FIFO worker disconnected before returning its outcome");
            }
        };
        worker.join().expect("FIFO worker panicked");

        let (fifo_failure, fifo_retries, fifo_offsets, fifo_substituted) = fifo_result;
        assert!(returned_without_release);
        assert!(fifo_substituted);
        assert_eq!(fifo_failure, DownloadFailure::Durability);
        assert_eq!(fifo_retries, 0);
        assert_eq!(fifo_offsets, [None]);
        assert_eq!(
            fifo_body_polls.load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        assert_eq!(std::fs::read(&fifo_original).unwrap(), b"");
        assert_eq!(
            std::fs::metadata(&fifo_original).unwrap().ino(),
            fifo_original_inode
        );
        assert!(std::fs::metadata(&fifo_part)
            .unwrap()
            .file_type()
            .is_fifo());
        assert!(!fifo_dir.path().join("model.gguf").exists());
        assert!(!fifo_dir.path().join("model.gguf.part.restart").exists());
    }

    #[test]
    fn complete_part_hash_pause_and_completion_fence_are_controlled() {
        let bytes = vec![b'x'; 3 * 64 * 1024 + 17];
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("model.gguf.part"), &bytes).unwrap();
        let before = artifact_snapshot(dir.path());
        let transport = FakeTransport {
            responses: RefCell::new(Vec::new()),
            offsets: RefCell::new(Vec::new()),
        };
        let control_checks = Cell::new(0);
        let checkpoints = RefCell::new(Vec::new());

        let outcome = download_with_transport_controlled(
            &spec(&bytes),
            dir.path(),
            &transport,
            || {
                let next = control_checks.get() + 1;
                control_checks.set(next);
                next == 3
            },
            |_| {},
            RetryWait { observer: |_| panic!("complete-part hash Pause must not retry"), sleep: |_| std::future::ready(()) },
            |operation| {
                match &operation {
                    ArtifactOperation::Sync { checkpoint, .. }
                    | ArtifactOperation::Observe(checkpoint) => {
                        checkpoints.borrow_mut().push(*checkpoint);
                    }
                    ArtifactOperation::Write { .. } => {
                        panic!("complete part must not write transfer bytes")
                    }
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap();

        assert_eq!(
            outcome,
            DownloadTerminalOutcome::Paused {
                retained_bytes: bytes.len() as u64,
            }
        );
        assert_eq!(control_checks.get(), 3);
        assert!(transport.offsets.borrow().is_empty());
        assert_eq!(artifact_snapshot(dir.path()), before);
        assert_eq!(
            &*checkpoints.borrow(),
            &[
                ArtifactCheckpoint::StagingSynced,
                ArtifactCheckpoint::StagingSynced,
                ArtifactCheckpoint::StagingIdentityMatched,
                ArtifactCheckpoint::DirectorySynced,
                ArtifactCheckpoint::NormalizationDirectorySynced,
                ArtifactCheckpoint::AuthoritativeRestatted,
            ]
        );

        let before_fence_dir = tempdir().unwrap();
        std::fs::write(before_fence_dir.path().join("model.gguf.part"), &bytes).unwrap();
        let before_fence_snapshot = artifact_snapshot(before_fence_dir.path());
        let before_fence_transport = FakeTransport {
            responses: RefCell::new(Vec::new()),
            offsets: RefCell::new(Vec::new()),
        };
        let pause_before_fence = Cell::new(false);
        let before_fence_checkpoints = RefCell::new(Vec::new());

        let before_fence = download_with_transport_controlled(
            &spec(&bytes),
            before_fence_dir.path(),
            &before_fence_transport,
            || pause_before_fence.get(),
            |_| {},
            RetryWait { observer: |_| panic!("complete-part pre-fence Pause must not retry"), sleep: |_| std::future::ready(()) },
            |operation| {
                match &operation {
                    ArtifactOperation::Sync { checkpoint, .. }
                    | ArtifactOperation::Observe(checkpoint) => {
                        before_fence_checkpoints.borrow_mut().push(*checkpoint);
                        if *checkpoint == ArtifactCheckpoint::BeforePromotion {
                            pause_before_fence.set(true);
                        }
                    }
                    ArtifactOperation::Write { .. } => {
                        panic!("complete part must not write transfer bytes")
                    }
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap();

        assert_eq!(
            before_fence,
            DownloadTerminalOutcome::Paused {
                retained_bytes: bytes.len() as u64,
            }
        );
        assert!(before_fence_transport.offsets.borrow().is_empty());
        assert_eq!(
            artifact_snapshot(before_fence_dir.path()),
            before_fence_snapshot
        );
        assert_eq!(
            before_fence_checkpoints.borrow().last(),
            Some(&ArtifactCheckpoint::AuthoritativeRestatted)
        );
        assert!(before_fence_checkpoints
            .borrow()
            .contains(&ArtifactCheckpoint::BeforePromotion));
        assert!(!before_fence_checkpoints
            .borrow()
            .contains(&ArtifactCheckpoint::CompletionFencePassed));

        let pause_debris_dir = tempdir().unwrap();
        std::fs::write(pause_debris_dir.path().join("model.gguf.part"), &bytes).unwrap();
        let pause_debris_transport = FakeTransport {
            responses: RefCell::new(Vec::new()),
            offsets: RefCell::new(Vec::new()),
        };
        let pause_with_debris = Cell::new(false);
        let pause_debris = download_with_transport_controlled(
            &spec(&bytes),
            pause_debris_dir.path(),
            &pause_debris_transport,
            || pause_with_debris.get(),
            |_| {},
            RetryWait { observer: |_| panic!("complete-part repair-debris Pause must not retry"), sleep: |_| std::future::ready(()) },
            |operation| {
                if matches!(
                    &operation,
                    ArtifactOperation::Observe(ArtifactCheckpoint::BeforePromotion)
                ) {
                    std::fs::write(
                        pause_debris_dir.path().join("model.gguf.invalid"),
                        b"late repair debris",
                    )
                    .unwrap();
                    pause_with_debris.set(true);
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();
        assert_eq!(pause_debris, DownloadFailure::Durability);
        assert!(pause_debris_transport.offsets.borrow().is_empty());
        assert_eq!(
            std::fs::read(pause_debris_dir.path().join("model.gguf.part")).unwrap(),
            bytes
        );
        assert_eq!(
            std::fs::read(pause_debris_dir.path().join("model.gguf.invalid")).unwrap(),
            b"late repair debris"
        );
        assert!(!pause_debris_dir.path().join("model.gguf").exists());

        let hash_debris_dir = tempdir().unwrap();
        std::fs::write(hash_debris_dir.path().join("model.gguf.part"), &bytes).unwrap();
        let hash_debris_transport = FakeTransport {
            responses: RefCell::new(Vec::new()),
            offsets: RefCell::new(Vec::new()),
        };
        let hash_checks = Cell::new(0);
        let first_sync = Cell::new(true);
        let hash_debris = download_with_transport_controlled(
            &spec(&bytes),
            hash_debris_dir.path(),
            &hash_debris_transport,
            || {
                let next = hash_checks.get() + 1;
                hash_checks.set(next);
                next == 3
            },
            |_| {},
            RetryWait { observer: |_| panic!("complete-part hash debris must not retry"), sleep: |_| std::future::ready(()) },
            |operation| {
                if matches!(
                    &operation,
                    ArtifactOperation::Sync {
                        checkpoint: ArtifactCheckpoint::StagingSynced,
                        ..
                    }
                ) && first_sync.replace(false)
                {
                    std::fs::write(
                        hash_debris_dir.path().join("model.gguf.invalid"),
                        b"hash-time repair debris",
                    )
                    .unwrap();
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();
        assert_eq!(hash_debris, DownloadFailure::Durability);
        assert!(hash_debris_transport.offsets.borrow().is_empty());
        assert_eq!(
            std::fs::read(hash_debris_dir.path().join("model.gguf.part")).unwrap(),
            bytes
        );
        assert_eq!(
            std::fs::read(hash_debris_dir.path().join("model.gguf.invalid")).unwrap(),
            b"hash-time repair debris"
        );
        assert!(!hash_debris_dir.path().join("model.gguf").exists());

        let late_dir = tempdir().unwrap();
        std::fs::write(late_dir.path().join("model.gguf.part"), &bytes).unwrap();
        let late_transport = FakeTransport {
            responses: RefCell::new(Vec::new()),
            offsets: RefCell::new(Vec::new()),
        };
        let late_pause = Cell::new(false);
        let late_checkpoints = RefCell::new(Vec::new());

        let late = download_with_transport_controlled(
            &spec(&bytes),
            late_dir.path(),
            &late_transport,
            || late_pause.get(),
            |_| {},
            RetryWait { observer: |_| panic!("complete-part promotion must not retry"), sleep: |_| std::future::ready(()) },
            |operation| {
                match &operation {
                    ArtifactOperation::Sync { checkpoint, .. }
                    | ArtifactOperation::Observe(checkpoint) => {
                        late_checkpoints.borrow_mut().push(*checkpoint);
                        if *checkpoint == ArtifactCheckpoint::CompletionFencePassed {
                            late_pause.set(true);
                        }
                    }
                    ArtifactOperation::Write { .. } => {
                        panic!("complete part must not write transfer bytes")
                    }
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap();

        assert!(late_pause.get());
        assert_eq!(
            late,
            DownloadTerminalOutcome::Complete(DownloadOutcome::Pulled(
                late_dir.path().join("model.gguf")
            ))
        );
        assert!(late_transport.offsets.borrow().is_empty());
        assert_eq!(
            std::fs::read(late_dir.path().join("model.gguf")).unwrap(),
            bytes
        );
        assert!(!late_dir.path().join("model.gguf.part").exists());

        let collision_dir = tempdir().unwrap();
        std::fs::write(collision_dir.path().join("model.gguf.part"), &bytes).unwrap();
        let collision_transport = FakeTransport {
            responses: RefCell::new(Vec::new()),
            offsets: RefCell::new(Vec::new()),
        };
        let collision_retries = Cell::new(0);

        let collision = download_with_transport_controlled(
            &spec(&bytes),
            collision_dir.path(),
            &collision_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| collision_retries.set(collision_retries.get() + 1), sleep: |_| std::future::ready(()) },
            |operation| {
                if matches!(
                    &operation,
                    ArtifactOperation::Observe(ArtifactCheckpoint::CompletionFencePassed)
                ) {
                    std::fs::write(collision_dir.path().join("model.gguf"), b"collision")
                        .unwrap();
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();

        assert_eq!(collision, DownloadFailure::Durability);
        assert_eq!(collision_retries.get(), 0);
        assert!(collision_transport.offsets.borrow().is_empty());
        assert_eq!(
            std::fs::read(collision_dir.path().join("model.gguf")).unwrap(),
            b"collision"
        );
        assert_eq!(
            std::fs::read(collision_dir.path().join("model.gguf.part")).unwrap(),
            bytes
        );

        let part_substitution_dir = tempdir().unwrap();
        let part_path = part_substitution_dir.path().join("model.gguf.part");
        let original_part = part_substitution_dir.path().join("pre-promotion-part");
        std::fs::write(&part_path, &bytes).unwrap();
        let part_substitution_transport = FakeTransport {
            responses: RefCell::new(Vec::new()),
            offsets: RefCell::new(Vec::new()),
        };
        let part_substituted = Cell::new(false);

        let part_substitution = download_with_transport_controlled(
            &spec(&bytes),
            part_substitution_dir.path(),
            &part_substitution_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| panic!("complete-part substitution must not retry"), sleep: |_| std::future::ready(()) },
            |operation| {
                if matches!(
                    &operation,
                    ArtifactOperation::Observe(ArtifactCheckpoint::CompletionFencePassed)
                ) {
                    std::fs::rename(&part_path, &original_part).unwrap();
                    std::fs::write(&part_path, &bytes).unwrap();
                    part_substituted.set(true);
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();

        assert!(part_substituted.get());
        assert_eq!(part_substitution, DownloadFailure::Durability);
        assert!(part_substitution_transport.offsets.borrow().is_empty());
        assert_eq!(std::fs::read(&original_part).unwrap(), bytes);
        assert_eq!(std::fs::read(&part_path).unwrap(), bytes);
        assert!(!part_substitution_dir.path().join("model.gguf").exists());

        let sync_failure_dir = tempdir().unwrap();
        std::fs::write(sync_failure_dir.path().join("model.gguf.part"), &bytes).unwrap();
        let sync_failure_transport = FakeTransport {
            responses: RefCell::new(Vec::new()),
            offsets: RefCell::new(Vec::new()),
        };
        let sync_failure = download_with_transport_controlled(
            &spec(&bytes),
            sync_failure_dir.path(),
            &sync_failure_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| panic!("promotion sync failure must not retry"), sleep: |_| std::future::ready(()) },
            |operation| {
                if matches!(
                    &operation,
                    ArtifactOperation::Sync {
                        checkpoint: ArtifactCheckpoint::PromotionDirectorySynced,
                        ..
                    }
                ) {
                    return Err(ArtifactOperationFailure::Other);
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();
        assert_eq!(sync_failure, DownloadFailure::Durability);
        assert!(sync_failure_transport.offsets.borrow().is_empty());
        assert_eq!(
            std::fs::read(sync_failure_dir.path().join("model.gguf")).unwrap(),
            bytes
        );
        assert!(!sync_failure_dir.path().join("model.gguf.part").exists());

        let final_substitution_dir = tempdir().unwrap();
        let final_path = final_substitution_dir.path().join("model.gguf");
        let original_final = final_substitution_dir.path().join("promoted-original");
        std::fs::write(
            final_substitution_dir.path().join("model.gguf.part"),
            &bytes,
        )
        .unwrap();
        let final_substitution_transport = FakeTransport {
            responses: RefCell::new(Vec::new()),
            offsets: RefCell::new(Vec::new()),
        };
        let final_substituted = Cell::new(false);
        let replacement = vec![b'y'; bytes.len()];
        let final_substitution = download_with_transport_controlled(
            &spec(&bytes),
            final_substitution_dir.path(),
            &final_substitution_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| panic!("final substitution must not retry"), sleep: |_| std::future::ready(()) },
            |operation| {
                if matches!(
                    &operation,
                    ArtifactOperation::Sync {
                        checkpoint: ArtifactCheckpoint::PromotionDirectorySynced,
                        ..
                    }
                ) {
                    std::fs::rename(&final_path, &original_final).unwrap();
                    std::fs::write(&final_path, &replacement).unwrap();
                    final_substituted.set(true);
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();
        assert!(final_substituted.get());
        assert_eq!(final_substitution, DownloadFailure::Durability);
        assert!(final_substitution_transport.offsets.borrow().is_empty());
        assert_eq!(std::fs::read(&original_final).unwrap(), bytes);
        assert_eq!(std::fs::read(&final_path).unwrap(), replacement);

        let swap_root = tempdir().unwrap();
        let swap_model = swap_root.path().join("model");
        let swap_moved = swap_root.path().join("moved-model");
        std::fs::create_dir(&swap_model).unwrap();
        std::fs::write(swap_model.join("model.gguf.part"), &bytes).unwrap();
        std::fs::write(swap_model.join("foreign.bin"), b"original foreign").unwrap();
        let swap_transport = FakeTransport {
            responses: RefCell::new(Vec::new()),
            offsets: RefCell::new(Vec::new()),
        };
        let directory_swapped = Cell::new(false);
        let swap = download_with_transport_controlled(
            &spec(&bytes),
            &swap_model,
            &swap_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| panic!("promotion directory swap must not retry"), sleep: |_| std::future::ready(()) },
            |operation| {
                if matches!(
                    &operation,
                    ArtifactOperation::Sync {
                        checkpoint: ArtifactCheckpoint::PromotionDirectorySynced,
                        ..
                    }
                ) {
                    std::fs::rename(&swap_model, &swap_moved).unwrap();
                    std::fs::create_dir(&swap_model).unwrap();
                    std::fs::write(swap_model.join("foreign.bin"), b"replacement foreign")
                        .unwrap();
                    directory_swapped.set(true);
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();
        assert!(directory_swapped.get());
        assert_eq!(swap, DownloadFailure::Durability);
        assert!(swap_transport.offsets.borrow().is_empty());
        assert_eq!(
            std::fs::read(swap_moved.join("model.gguf")).unwrap(),
            bytes
        );
        assert_eq!(
            std::fs::read(swap_moved.join("foreign.bin")).unwrap(),
            b"original foreign"
        );
        assert_eq!(
            std::fs::read(swap_model.join("foreign.bin")).unwrap(),
            b"replacement foreign"
        );

        for debris_name in ["model.gguf.invalid", "model.gguf.part.restart"] {
            let debris_dir = tempdir().unwrap();
            std::fs::write(debris_dir.path().join("model.gguf.part"), &bytes).unwrap();
            std::fs::write(debris_dir.path().join(debris_name), b"repair debris").unwrap();
            let debris_before = artifact_snapshot(debris_dir.path());
            let debris_transport = FakeTransport {
                responses: RefCell::new(Vec::new()),
                offsets: RefCell::new(Vec::new()),
            };
            let debris = download_with_transport_controlled(
                &spec(&bytes),
                debris_dir.path(),
                &debris_transport,
                || false,
                |_| {},
                RetryWait { observer: |_| panic!("complete part with repair debris must not retry"), sleep: |_| std::future::ready(()) },
                perform_artifact_operation,
            )
            .unwrap_err();
            assert_eq!(debris, DownloadFailure::Durability);
            assert!(debris_transport.offsets.borrow().is_empty());
            assert_eq!(artifact_snapshot(debris_dir.path()), debris_before);
        }

        for debris_name in ["model.gguf.invalid", "model.gguf.part.restart"] {
            let sticky_dir = tempdir().unwrap();
            let debris_path = sticky_dir.path().join(debris_name);
            let moved_debris = sticky_dir.path().join("removed-repair-evidence");
            std::fs::write(sticky_dir.path().join("model.gguf.part"), &bytes).unwrap();
            std::fs::write(&debris_path, b"sticky repair evidence").unwrap();
            let sticky_transport = FakeTransport {
                responses: RefCell::new(Vec::new()),
                offsets: RefCell::new(Vec::new()),
            };
            let first_sync_hook_ran = Cell::new(false);
            let sticky = download_with_transport_controlled(
                &spec(&bytes),
                sticky_dir.path(),
                &sticky_transport,
                || false,
                |_| {},
                RetryWait { observer: |_| panic!("sticky repair authority must not retry"), sleep: |_| std::future::ready(()) },
                |operation| {
                    if matches!(
                        &operation,
                        ArtifactOperation::Sync {
                            checkpoint: ArtifactCheckpoint::StagingSynced,
                            ..
                        }
                    ) && !first_sync_hook_ran.replace(true)
                    {
                        std::fs::rename(&debris_path, &moved_debris).unwrap();
                    }
                    perform_artifact_operation(operation)
                },
            )
            .unwrap_err();
            assert_eq!(sticky, DownloadFailure::Durability);
            assert!(!first_sync_hook_ran.get());
            assert!(sticky_transport.offsets.borrow().is_empty());
            assert_eq!(
                std::fs::read(sticky_dir.path().join("model.gguf.part")).unwrap(),
                bytes
            );
            assert_eq!(std::fs::read(&debris_path).unwrap(), b"sticky repair evidence");
            assert!(!moved_debris.exists());
            assert!(!sticky_dir.path().join("model.gguf").exists());
        }

        let late_debris_dir = tempdir().unwrap();
        std::fs::write(late_debris_dir.path().join("model.gguf.part"), &bytes).unwrap();
        let late_debris_transport = FakeTransport {
            responses: RefCell::new(Vec::new()),
            offsets: RefCell::new(Vec::new()),
        };
        let late_debris = download_with_transport_controlled(
            &spec(&bytes),
            late_debris_dir.path(),
            &late_debris_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| panic!("late repair debris must not retry"), sleep: |_| std::future::ready(()) },
            |operation| {
                if matches!(
                    &operation,
                    ArtifactOperation::Observe(ArtifactCheckpoint::CompletionFencePassed)
                ) {
                    std::fs::write(
                        late_debris_dir.path().join("model.gguf.invalid"),
                        b"late repair debris",
                    )
                    .unwrap();
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();
        assert_eq!(late_debris, DownloadFailure::Durability);
        assert!(late_debris_transport.offsets.borrow().is_empty());
        assert_eq!(
            std::fs::read(late_debris_dir.path().join("model.gguf.part")).unwrap(),
            bytes
        );
        assert_eq!(
            std::fs::read(late_debris_dir.path().join("model.gguf.invalid")).unwrap(),
            b"late repair debris"
        );
        assert!(!late_debris_dir.path().join("model.gguf").exists());
    }

    #[test]
    fn invalid_complete_part_cleanup_is_descriptor_bound_before_integrity() {
        let expected = b"abcdef";
        let received = b"abcdeg";
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("model.gguf.part"), received).unwrap();
        std::fs::write(dir.path().join("model.lock"), b"catalog authority").unwrap();
        std::fs::write(dir.path().join("foreign.bin"), b"foreign").unwrap();
        let transport = FakeTransport {
            responses: RefCell::new(Vec::new()),
            offsets: RefCell::new(Vec::new()),
        };
        let retries = Cell::new(0);
        let checkpoints = RefCell::new(Vec::new());

        let failure = download_with_transport_controlled(
            &spec(expected),
            dir.path(),
            &transport,
            || false,
            |_| {},
            RetryWait { observer: |_| retries.set(retries.get() + 1), sleep: |_| std::future::ready(()) },
            |operation| {
                match &operation {
                    ArtifactOperation::Sync { checkpoint, .. }
                    | ArtifactOperation::Observe(checkpoint) => {
                        checkpoints.borrow_mut().push(*checkpoint);
                    }
                    ArtifactOperation::Write { .. } => {
                        panic!("complete part must not write transfer bytes")
                    }
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();

        assert_eq!(
            failure,
            DownloadFailure::Integrity {
                retained_bytes: 0,
                authority: IntegrityAuthority::PendingOnly,
            }
        );
        assert_eq!(retries.get(), 0);
        assert!(transport.offsets.borrow().is_empty());
        assert!(!dir.path().join("model.gguf.part").exists());
        assert!(!dir.path().join("model.gguf").exists());
        assert!(!dir.path().join("model.gguf.part.restart").exists());
        assert!(!dir.path().join("model.gguf.invalid").exists());
        assert_eq!(
            std::fs::read(dir.path().join("model.lock")).unwrap(),
            b"catalog authority"
        );
        assert_eq!(
            std::fs::read(dir.path().join("foreign.bin")).unwrap(),
            b"foreign"
        );
        assert!(checkpoints
            .borrow()
            .contains(&ArtifactCheckpoint::ChecksumCleanupDirectorySynced));
        assert_eq!(
            checkpoints.borrow().last(),
            Some(&ArtifactCheckpoint::BeforeIntegrityAuthorityObservation)
        );

        let substitution_dir = tempdir().unwrap();
        let part = substitution_dir.path().join("model.gguf.part");
        let original = substitution_dir.path().join("pre-cleanup-part");
        std::fs::write(&part, received).unwrap();
        let substitution_transport = FakeTransport {
            responses: RefCell::new(Vec::new()),
            offsets: RefCell::new(Vec::new()),
        };
        let substituted = Cell::new(false);
        let substitution = download_with_transport_controlled(
            &spec(expected),
            substitution_dir.path(),
            &substitution_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| panic!("complete-part cleanup substitution must not retry"), sleep: |_| std::future::ready(()) },
            |operation| {
                if matches!(
                    &operation,
                    ArtifactOperation::Observe(ArtifactCheckpoint::AuthoritativePartIdentityMatched)
                ) {
                    std::fs::rename(&part, &original).unwrap();
                    std::fs::write(&part, received).unwrap();
                    substituted.set(true);
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();
        assert!(substituted.get());
        assert_eq!(substitution, DownloadFailure::Durability);
        assert!(substitution_transport.offsets.borrow().is_empty());
        assert_eq!(std::fs::read(&original).unwrap(), received);
        assert_eq!(std::fs::read(&part).unwrap(), received);
        assert!(!substitution_dir.path().join("model.gguf").exists());

        let sync_failure_dir = tempdir().unwrap();
        std::fs::write(sync_failure_dir.path().join("model.gguf.part"), received).unwrap();
        let sync_failure_transport = FakeTransport {
            responses: RefCell::new(Vec::new()),
            offsets: RefCell::new(Vec::new()),
        };
        let sync_failure = download_with_transport_controlled(
            &spec(expected),
            sync_failure_dir.path(),
            &sync_failure_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| panic!("complete-part cleanup sync failure must not retry"), sleep: |_| std::future::ready(()) },
            |operation| {
                if matches!(
                    &operation,
                    ArtifactOperation::Sync {
                        checkpoint: ArtifactCheckpoint::ChecksumCleanupDirectorySynced,
                        ..
                    }
                ) {
                    return Err(ArtifactOperationFailure::Other);
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();
        assert_eq!(sync_failure, DownloadFailure::Durability);
        assert!(sync_failure_transport.offsets.borrow().is_empty());
        assert!(!sync_failure_dir.path().join("model.gguf").exists());

        let repair_dir = tempdir().unwrap();
        std::fs::write(repair_dir.path().join("model.gguf.part"), received).unwrap();
        let repair_transport = FakeTransport {
            responses: RefCell::new(Vec::new()),
            offsets: RefCell::new(Vec::new()),
        };
        let first_sync = Cell::new(true);
        let repair = download_with_transport_controlled(
            &spec(expected),
            repair_dir.path(),
            &repair_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| panic!("complete-part repair authority must not retry"), sleep: |_| std::future::ready(()) },
            |operation| {
                if matches!(
                    &operation,
                    ArtifactOperation::Sync {
                        checkpoint: ArtifactCheckpoint::StagingSynced,
                        ..
                    }
                ) && first_sync.replace(false)
                {
                    std::fs::write(
                        repair_dir.path().join("model.gguf.invalid"),
                        b"late repair evidence",
                    )
                    .unwrap();
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();
        assert_eq!(
            repair,
            DownloadFailure::Integrity {
                retained_bytes: 0,
                authority: IntegrityAuthority::Repair,
            }
        );
        assert!(repair_transport.offsets.borrow().is_empty());
        assert!(!repair_dir.path().join("model.gguf.part").exists());
        assert_eq!(
            std::fs::read(repair_dir.path().join("model.gguf.invalid")).unwrap(),
            b"late repair evidence"
        );
    }

    #[test]
    fn oversize_part_cleanup_is_durable_before_zero_offset_request() {
        let expected = b"abcdef";
        let oversized = b"abcdefg";
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("model.gguf.part"), oversized).unwrap();
        std::fs::write(dir.path().join("model.lock"), b"catalog authority").unwrap();
        std::fs::write(dir.path().join("foreign.bin"), b"foreign").unwrap();
        let cleanup_complete = Cell::new(false);
        let transport = CleanupObservingTransport {
            cleanup_complete: &cleanup_complete,
            observed_cleanup: Cell::new(false),
            response: RefCell::new(Some(transfer(StatusCode::OK, None, expected))),
            offsets: RefCell::new(Vec::new()),
        };
        let checkpoints = RefCell::new(Vec::new());

        let outcome = download_with_transport_controlled(
            &spec(expected),
            dir.path(),
            &transport,
            || false,
            |_| {},
            RetryWait { observer: |_| panic!("oversize cleanup success must not retry"), sleep: |_| std::future::ready(()) },
            |operation| {
                match &operation {
                    ArtifactOperation::Sync { checkpoint, .. }
                    | ArtifactOperation::Observe(checkpoint) => {
                        checkpoints.borrow_mut().push(*checkpoint);
                        if *checkpoint == ArtifactCheckpoint::AuthoritativePartAbsent {
                            cleanup_complete.set(true);
                        }
                    }
                    ArtifactOperation::Write { .. } => {}
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap();

        assert_eq!(
            outcome,
            DownloadTerminalOutcome::Complete(DownloadOutcome::Pulled(
                dir.path().join("model.gguf")
            ))
        );
        assert!(cleanup_complete.get());
        assert!(transport.observed_cleanup.get());
        assert_eq!(&*transport.offsets.borrow(), &[None]);
        assert_eq!(std::fs::read(dir.path().join("model.gguf")).unwrap(), expected);
        assert!(!dir.path().join("model.gguf.part").exists());
        assert_eq!(
            std::fs::read(dir.path().join("model.lock")).unwrap(),
            b"catalog authority"
        );
        assert_eq!(
            std::fs::read(dir.path().join("foreign.bin")).unwrap(),
            b"foreign"
        );
        assert!(checkpoints
            .borrow()
            .contains(&ArtifactCheckpoint::ChecksumCleanupDirectorySynced));

        let substitution_dir = tempdir().unwrap();
        let part = substitution_dir.path().join("model.gguf.part");
        let original = substitution_dir.path().join("oversize-original");
        std::fs::write(&part, oversized).unwrap();
        let substitution_transport = FakeTransport {
            responses: RefCell::new(Vec::new()),
            offsets: RefCell::new(Vec::new()),
        };
        let substituted = Cell::new(false);
        let substitution = download_with_transport_controlled(
            &spec(expected),
            substitution_dir.path(),
            &substitution_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| panic!("oversize substitution must not retry"), sleep: |_| std::future::ready(()) },
            |operation| {
                if matches!(
                    &operation,
                    ArtifactOperation::Observe(ArtifactCheckpoint::AuthoritativePartIdentityMatched)
                ) {
                    std::fs::rename(&part, &original).unwrap();
                    std::fs::write(&part, oversized).unwrap();
                    substituted.set(true);
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();
        assert!(substituted.get());
        assert_eq!(substitution, DownloadFailure::Durability);
        assert!(substitution_transport.offsets.borrow().is_empty());
        assert_eq!(std::fs::read(&original).unwrap(), oversized);
        assert_eq!(std::fs::read(&part).unwrap(), oversized);
        assert!(!substitution_dir.path().join("model.gguf").exists());

        #[cfg(unix)]
        {
            let unsafe_dir = tempdir().unwrap();
            let unsafe_part = unsafe_dir.path().join("model.gguf.part");
            let unsafe_original = unsafe_dir.path().join("oversize-original");
            let outside = unsafe_dir.path().join("outside");
            std::fs::write(&unsafe_part, oversized).unwrap();
            std::fs::write(&outside, b"outside").unwrap();
            let unsafe_transport = FakeTransport {
                responses: RefCell::new(Vec::new()),
                offsets: RefCell::new(Vec::new()),
            };
            let unsafe_substitution = download_with_transport_controlled(
                &spec(expected),
                unsafe_dir.path(),
                &unsafe_transport,
                || false,
                |_| {},
                RetryWait { observer: |_| panic!("unsafe oversize substitution must not retry"), sleep: |_| std::future::ready(()) },
                |operation| {
                    if matches!(
                        &operation,
                        ArtifactOperation::Observe(
                            ArtifactCheckpoint::AuthoritativePartIdentityMatched
                        )
                    ) {
                        std::fs::rename(&unsafe_part, &unsafe_original).unwrap();
                        std::os::unix::fs::symlink(&outside, &unsafe_part).unwrap();
                    }
                    perform_artifact_operation(operation)
                },
            )
            .unwrap_err();
            assert_eq!(unsafe_substitution, DownloadFailure::Durability);
            assert!(unsafe_transport.offsets.borrow().is_empty());
            assert_eq!(std::fs::read(&unsafe_original).unwrap(), oversized);
            assert_eq!(std::fs::read(&outside).unwrap(), b"outside");
            assert!(std::fs::symlink_metadata(&unsafe_part)
                .unwrap()
                .file_type()
                .is_symlink());
            assert!(!unsafe_dir.path().join("model.gguf").exists());
        }

        let removal_dir = tempdir().unwrap();
        let removal_part = removal_dir.path().join("model.gguf.part");
        let removed_original = removal_dir.path().join("removed-oversize-part");
        std::fs::write(&removal_part, oversized).unwrap();
        let removal_transport = FakeTransport {
            responses: RefCell::new(Vec::new()),
            offsets: RefCell::new(Vec::new()),
        };
        let removal = download_with_transport_controlled(
            &spec(expected),
            removal_dir.path(),
            &removal_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| panic!("oversize removal must not retry"), sleep: |_| std::future::ready(()) },
            |operation| {
                if matches!(
                    &operation,
                    ArtifactOperation::Observe(ArtifactCheckpoint::BeforeAuthoritativePartUnlink)
                ) {
                    std::fs::rename(&removal_part, &removed_original).unwrap();
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();
        assert_eq!(removal, DownloadFailure::Durability);
        assert!(removal_transport.offsets.borrow().is_empty());
        assert_eq!(std::fs::read(&removed_original).unwrap(), oversized);
        assert!(!removal_part.exists());
        assert!(!removal_dir.path().join("model.gguf").exists());

        let sync_failure_dir = tempdir().unwrap();
        std::fs::write(sync_failure_dir.path().join("model.gguf.part"), oversized).unwrap();
        let sync_failure_transport = FakeTransport {
            responses: RefCell::new(Vec::new()),
            offsets: RefCell::new(Vec::new()),
        };
        let sync_failure = download_with_transport_controlled(
            &spec(expected),
            sync_failure_dir.path(),
            &sync_failure_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| panic!("oversize cleanup sync failure must not retry"), sleep: |_| std::future::ready(()) },
            |operation| {
                if matches!(
                    &operation,
                    ArtifactOperation::Sync {
                        checkpoint: ArtifactCheckpoint::ChecksumCleanupDirectorySynced,
                        ..
                    }
                ) {
                    return Err(ArtifactOperationFailure::Other);
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();
        assert_eq!(sync_failure, DownloadFailure::Durability);
        assert!(sync_failure_transport.offsets.borrow().is_empty());
        assert!(!sync_failure_dir.path().join("model.gguf").exists());
    }

    #[test]
    fn response_overrun_cleanup_is_descriptor_bound_and_preserves_only_proven_recovery() {
        let expected = b"abcdef";
        let overrun = b"abcdefg";

        let fresh_dir = tempdir().unwrap();
        std::fs::write(fresh_dir.path().join("model.gguf.invalid"), b"repair evidence").unwrap();
        std::fs::write(fresh_dir.path().join("model.lock"), b"catalog authority").unwrap();
        std::fs::write(fresh_dir.path().join("foreign.bin"), b"foreign").unwrap();
        let fresh_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, overrun)]),
            offsets: RefCell::new(Vec::new()),
        };
        let fresh_retries = Cell::new(0);
        let mut fresh_progress = Vec::new();
        let fresh = download_with_transport_controlled(
            &spec(expected),
            fresh_dir.path(),
            &fresh_transport,
            || false,
            |event| fresh_progress.push(event),
            RetryWait { observer: |_| fresh_retries.set(fresh_retries.get() + 1), sleep: |_| std::future::ready(()) },
            perform_artifact_operation,
        )
        .unwrap_err();
        assert_eq!(fresh, DownloadFailure::Remote { retained_bytes: 0 });
        assert_eq!(fresh_retries.get(), 0);
        assert_eq!(&*fresh_transport.offsets.borrow(), &[None]);
        assert_eq!(
            fresh_progress,
            [
                ProgressUpdate::Transferring {
                    transferred: 0,
                    total: 6,
                },
                ProgressUpdate::Transferring {
                    transferred: 6,
                    total: 6,
                },
            ]
        );
        assert!(!fresh_dir.path().join("model.gguf.part").exists());
        assert!(!fresh_dir.path().join("model.gguf.part.restart").exists());
        assert!(!fresh_dir.path().join("model.gguf").exists());
        assert_eq!(
            std::fs::read(fresh_dir.path().join("model.gguf.invalid")).unwrap(),
            b"repair evidence"
        );
        assert_eq!(
            std::fs::read(fresh_dir.path().join("model.lock")).unwrap(),
            b"catalog authority"
        );
        assert_eq!(
            std::fs::read(fresh_dir.path().join("foreign.bin")).unwrap(),
            b"foreign"
        );

        let resume_dir = tempdir().unwrap();
        std::fs::write(resume_dir.path().join("model.gguf.part"), b"abc").unwrap();
        let resume_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(
                StatusCode::PARTIAL_CONTENT,
                Some("bytes 3-5/6"),
                b"defg",
            )]),
            offsets: RefCell::new(Vec::new()),
        };
        let resume_retries = Cell::new(0);
        let resume = download_with_transport_controlled(
            &spec(expected),
            resume_dir.path(),
            &resume_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| resume_retries.set(resume_retries.get() + 1), sleep: |_| std::future::ready(()) },
            perform_artifact_operation,
        )
        .unwrap_err();
        assert_eq!(resume, DownloadFailure::Remote { retained_bytes: 0 });
        assert_eq!(resume_retries.get(), 0);
        assert_eq!(&*resume_transport.offsets.borrow(), &[Some(3)]);
        assert!(!resume_dir.path().join("model.gguf.part").exists());
        assert!(!resume_dir.path().join("model.gguf.part.restart").exists());
        assert!(!resume_dir.path().join("model.gguf").exists());

        let restart_dir = tempdir().unwrap();
        std::fs::write(restart_dir.path().join("model.gguf.part"), b"abc").unwrap();
        let restart_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, overrun)]),
            offsets: RefCell::new(Vec::new()),
        };
        let restart_retries = Cell::new(0);
        let restart = download_with_transport_controlled(
            &spec(expected),
            restart_dir.path(),
            &restart_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| restart_retries.set(restart_retries.get() + 1), sleep: |_| std::future::ready(()) },
            perform_artifact_operation,
        )
        .unwrap_err();
        assert_eq!(restart, DownloadFailure::Remote { retained_bytes: 3 });
        assert_eq!(restart_retries.get(), 0);
        assert_eq!(&*restart_transport.offsets.borrow(), &[Some(3)]);
        assert_eq!(
            std::fs::read(restart_dir.path().join("model.gguf.part")).unwrap(),
            b"abc"
        );
        assert!(!restart_dir.path().join("model.gguf.part.restart").exists());
        assert!(!restart_dir.path().join("model.gguf").exists());

        let part_substitution_dir = tempdir().unwrap();
        let part = part_substitution_dir.path().join("model.gguf.part");
        let original_part = part_substitution_dir.path().join("overrun-original");
        std::fs::write(part_substitution_dir.path().join("model.lock"), b"catalog authority")
            .unwrap();
        let part_substitution_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, overrun)]),
            offsets: RefCell::new(Vec::new()),
        };
        let part_substituted = Cell::new(false);
        let part_substitution = download_with_transport_controlled(
            &spec(expected),
            part_substitution_dir.path(),
            &part_substitution_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| panic!("overrun part substitution must not retry"), sleep: |_| std::future::ready(()) },
            |operation| {
                if matches!(
                    &operation,
                    ArtifactOperation::Observe(ArtifactCheckpoint::AuthoritativePartIdentityMatched)
                ) {
                    std::fs::rename(&part, &original_part).unwrap();
                    std::fs::write(&part, overrun).unwrap();
                    std::fs::write(
                        part_substitution_dir.path().join("model.gguf"),
                        b"final witness",
                    )
                    .unwrap();
                    part_substituted.set(true);
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();
        assert!(part_substituted.get());
        assert_eq!(part_substitution, DownloadFailure::Durability);
        assert_eq!(&*part_substitution_transport.offsets.borrow(), &[None]);
        assert_eq!(std::fs::read(&original_part).unwrap(), overrun);
        assert_eq!(std::fs::read(&part).unwrap(), overrun);
        assert_eq!(
            std::fs::read(part_substitution_dir.path().join("model.gguf")).unwrap(),
            b"final witness"
        );
        assert_eq!(
            std::fs::read(part_substitution_dir.path().join("model.lock")).unwrap(),
            b"catalog authority"
        );

        let part_sync_dir = tempdir().unwrap();
        let part_sync_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, overrun)]),
            offsets: RefCell::new(Vec::new()),
        };
        let part_sync_retries = Cell::new(0);
        let part_sync_failure = download_with_transport_controlled(
            &spec(expected),
            part_sync_dir.path(),
            &part_sync_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| part_sync_retries.set(part_sync_retries.get() + 1), sleep: |_| std::future::ready(()) },
            |operation| {
                if matches!(
                    &operation,
                    ArtifactOperation::Sync {
                        checkpoint: ArtifactCheckpoint::ChecksumCleanupDirectorySynced,
                        ..
                    }
                ) {
                    return Err(ArtifactOperationFailure::Other);
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();
        assert_eq!(part_sync_failure, DownloadFailure::Durability);
        assert_eq!(part_sync_retries.get(), 0);
        assert_eq!(&*part_sync_transport.offsets.borrow(), &[None]);
        assert!(!part_sync_dir.path().join("model.gguf").exists());

        let restart_substitution_dir = tempdir().unwrap();
        std::fs::write(
            restart_substitution_dir.path().join("model.gguf.part"),
            b"abc",
        )
        .unwrap();
        let restart = restart_substitution_dir
            .path()
            .join("model.gguf.part.restart");
        let original_restart = restart_substitution_dir.path().join("overrun-restart-original");
        let restart_substitution_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, overrun)]),
            offsets: RefCell::new(Vec::new()),
        };
        let restart_substituted = Cell::new(false);
        let restart_substitution = download_with_transport_controlled(
            &spec(expected),
            restart_substitution_dir.path(),
            &restart_substitution_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| panic!("overrun restart substitution must not retry"), sleep: |_| std::future::ready(()) },
            |operation| {
                if matches!(
                    &operation,
                    ArtifactOperation::Observe(ArtifactCheckpoint::RestartIdentityMatched)
                ) {
                    std::fs::rename(&restart, &original_restart).unwrap();
                    std::fs::write(&restart, overrun).unwrap();
                    restart_substituted.set(true);
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();
        assert!(restart_substituted.get());
        assert_eq!(restart_substitution, DownloadFailure::Durability);
        assert_eq!(&*restart_substitution_transport.offsets.borrow(), &[Some(3)]);
        assert_eq!(
            std::fs::read(restart_substitution_dir.path().join("model.gguf.part")).unwrap(),
            b"abc"
        );
        assert_eq!(std::fs::read(&original_restart).unwrap(), overrun);
        assert_eq!(std::fs::read(&restart).unwrap(), overrun);
        assert!(!restart_substitution_dir.path().join("model.gguf").exists());

        let recovered_sync_dir = tempdir().unwrap();
        std::fs::write(recovered_sync_dir.path().join("model.gguf.part"), b"abc").unwrap();
        let recovered_sync_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, overrun)]),
            offsets: RefCell::new(Vec::new()),
        };
        let recovered_sync_retries = Cell::new(0);
        let recovered_sync_failure = download_with_transport_controlled(
            &spec(expected),
            recovered_sync_dir.path(),
            &recovered_sync_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| recovered_sync_retries.set(recovered_sync_retries.get() + 1), sleep: |_| std::future::ready(()) },
            |operation| {
                if matches!(
                    &operation,
                    ArtifactOperation::Sync {
                        checkpoint: ArtifactCheckpoint::RecoveredPartSynced,
                        ..
                    }
                ) {
                    return Err(ArtifactOperationFailure::Other);
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();
        assert_eq!(recovered_sync_failure, DownloadFailure::Durability);
        assert_eq!(recovered_sync_retries.get(), 0);
        assert_eq!(&*recovered_sync_transport.offsets.borrow(), &[Some(3)]);
        assert_eq!(
            std::fs::read(recovered_sync_dir.path().join("model.gguf.part")).unwrap(),
            b"abc"
        );
        assert!(!recovered_sync_dir
            .path()
            .join("model.gguf.part.restart")
            .exists());
        assert!(!recovered_sync_dir.path().join("model.gguf").exists());

        let stale_part_dir = tempdir().unwrap();
        let stale_part = stale_part_dir.path().join("model.gguf.part");
        let stale_original = stale_part_dir.path().join("overrun-old-part");
        std::fs::write(&stale_part, b"abc").unwrap();
        let stale_transport = FakeTransport {
            responses: RefCell::new(vec![transfer(StatusCode::OK, None, overrun)]),
            offsets: RefCell::new(Vec::new()),
        };
        let stale_part_replaced = Cell::new(false);
        let stale = download_with_transport_controlled(
            &spec(expected),
            stale_part_dir.path(),
            &stale_transport,
            || false,
            |_| {},
            RetryWait { observer: |_| panic!("stale overrun recovery must not retry"), sleep: |_| std::future::ready(()) },
            |operation| {
                if matches!(
                    &operation,
                    ArtifactOperation::Observe(ArtifactCheckpoint::BeforeRecoveredPartRestat)
                ) {
                    std::fs::rename(&stale_part, &stale_original).unwrap();
                    std::fs::write(&stale_part, b"xyz").unwrap();
                    stale_part_replaced.set(true);
                }
                perform_artifact_operation(operation)
            },
        )
        .unwrap_err();
        assert!(stale_part_replaced.get());
        assert_eq!(stale, DownloadFailure::Durability);
        assert_eq!(&*stale_transport.offsets.borrow(), &[Some(3)]);
        assert_eq!(std::fs::read(&stale_original).unwrap(), b"abc");
        assert_eq!(std::fs::read(&stale_part).unwrap(), b"xyz");
        assert!(!stale_part_dir
            .path()
            .join("model.gguf.part.restart")
            .exists());
        assert!(!stale_part_dir.path().join("model.gguf").exists());
    }
}
