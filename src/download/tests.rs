mod tests {
    use super::*;
    use crate::huggingface::ResolvedFile;
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

    impl Transport for FakeTransport {
        async fn get(&self, _url: &Url, offset: Option<u64>) -> Result<Transfer, TransferError> {
            self.offsets.borrow_mut().push(offset);
            let mut responses = self.responses.borrow_mut();
            if responses.is_empty() {
                Err(TransferError::fatal("no fake response"))
            } else {
                Ok(responses.remove(0))
            }
        }
    }

    impl Transport for FailingTransport {
        async fn get(&self, _url: &Url, offset: Option<u64>) -> Result<Transfer, TransferError> {
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
                ProgressUpdate::Seed(3),
                ProgressUpdate::Position(6),
                ProgressUpdate::Verifying,
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
            [ProgressUpdate::Verifying, ProgressUpdate::Seed(6)]
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

        let error = match test_runtime().block_on(transport.get(&url, None)) {
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
            let mut transfer = transport.get(&url, None).await.unwrap();
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
}
