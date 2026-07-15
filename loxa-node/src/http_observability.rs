use axum::{
    body::Body,
    extract::MatchedPath,
    http::{header::HeaderName, Request, Response},
    middleware::{self, Next},
    Router,
};
use std::time::Duration;
use tower_http::{
    request_id::{MakeRequestId, PropagateRequestIdLayer, RequestId, SetRequestIdLayer},
    trace::TraceLayer,
};
use tracing::{field, Level, Span};

const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DiagnosticRequestId(pub(crate) String);

trait RequestIdSource: Clone + Send + Sync + 'static {
    fn fill(&mut self, bytes: &mut [u8; 16]) -> Result<(), ()>;
}

#[derive(Clone, Copy, Debug, Default)]
struct OsRequestIdSource;

impl RequestIdSource for OsRequestIdSource {
    fn fill(&mut self, bytes: &mut [u8; 16]) -> Result<(), ()> {
        getrandom::fill(bytes).map_err(|_| ())
    }
}

#[derive(Clone, Copy, Debug)]
struct RandomRequestId<S>(S);

impl<S: RequestIdSource> MakeRequestId for RandomRequestId<S> {
    fn make_request_id<B>(&mut self, _request: &Request<B>) -> Option<RequestId> {
        let mut random = [0_u8; 16];
        self.0.fill(&mut random).ok()?;
        let mut encoded = [0_u8; 32];
        const LOWER_HEX: &[u8; 16] = b"0123456789abcdef";
        for (index, byte) in random.into_iter().enumerate() {
            encoded[index * 2] = LOWER_HEX[usize::from(byte >> 4)];
            encoded[index * 2 + 1] = LOWER_HEX[usize::from(byte & 0x0f)];
        }
        let value = std::str::from_utf8(&encoded)
            .expect("hexadecimal request ID is UTF-8")
            .parse()
            .expect("hexadecimal request ID is a valid header value");
        Some(RequestId::new(value))
    }
}

async fn discard_untrusted_request_id(mut request: Request<Body>) -> Request<Body> {
    request.headers_mut().remove(&REQUEST_ID_HEADER);
    request.extensions_mut().remove::<RequestId>();
    request.extensions_mut().remove::<DiagnosticRequestId>();
    request
}

async fn attach_diagnostic_request_id(mut request: Request<Body>) -> Request<Body> {
    if let Some(trusted) = request.extensions().get::<RequestId>() {
        let trusted = trusted
            .header_value()
            .to_str()
            .expect("generated request ID is ASCII")
            .to_owned();
        request
            .extensions_mut()
            .insert(DiagnosticRequestId(trusted));
    }
    request
}

async fn enforce_trusted_response_id(request: Request<Body>, next: Next) -> Response<Body> {
    let trusted = request
        .extensions()
        .get::<RequestId>()
        .map(|request_id| request_id.header_value().clone());
    let mut response = next.run(request).await;
    if let Some(trusted) = trusted {
        response
            .headers_mut()
            .insert(REQUEST_ID_HEADER.clone(), trusted);
    }
    response
}

fn elapsed_milliseconds(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn result_class(status: axum::http::StatusCode) -> &'static str {
    if status.is_server_error() {
        "server_error"
    } else if status.is_client_error() {
        "client_error"
    } else {
        "success"
    }
}

pub(crate) fn apply(router: Router) -> Router {
    apply_with_source(router, OsRequestIdSource)
}

fn apply_with_source<S: RequestIdSource>(router: Router, source: S) -> Router {
    // Request order is intentionally the reverse of these calls:
    // discard untrusted context -> generate -> attach -> trace -> propagate.
    // On the response path, the request-aware middleware overwrites a
    // route-local value only when generation produced a trusted ID.
    router
        .layer(PropagateRequestIdLayer::new(REQUEST_ID_HEADER.clone()))
        .layer(middleware::from_fn(enforce_trusted_response_id))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|request: &Request<Body>| {
                    let Some(request_id) = request.extensions().get::<DiagnosticRequestId>() else {
                        return Span::none();
                    };
                    let route = request
                        .extensions()
                        .get::<MatchedPath>()
                        .map(MatchedPath::as_str)
                        .unwrap_or("unmatched");
                    tracing::span!(
                        target: "loxa_node::http",
                        Level::INFO,
                        "http.request",
                        request_id = request_id.0.as_str(),
                        method = request.method().as_str(),
                        route,
                        status = field::Empty,
                        latency_ms = field::Empty,
                        result_class = field::Empty,
                    )
                })
                .on_request(())
                .on_response(
                    |response: &Response<Body>, latency: Duration, span: &Span| {
                        if span.is_disabled() {
                            return;
                        }
                        let status = response.status().as_u16();
                        let latency_ms = elapsed_milliseconds(latency);
                        let result_class = result_class(response.status());
                        span.record("status", status);
                        span.record("latency_ms", latency_ms);
                        span.record("result_class", result_class);
                        tracing::event!(
                            target: "loxa_node::http",
                            parent: span,
                            Level::INFO,
                            event_code = "http.request.completed",
                            component = "http",
                            status,
                            latency_ms,
                            result_class,
                        );
                    },
                )
                .on_body_chunk(())
                .on_eos(())
                .on_failure(()),
        )
        .layer(middleware::map_request(attach_diagnostic_request_id))
        .layer(SetRequestIdLayer::new(
            REQUEST_ID_HEADER.clone(),
            RandomRequestId(source),
        ))
        .layer(middleware::map_request(discard_untrusted_request_id))
}

#[cfg(test)]
mod tests {
    use axum::{
        body::{to_bytes, Body},
        extract::{Extension, State},
        http::{header, HeaderMap, Request, StatusCode},
        response::{sse::Event, IntoResponse, Sse},
        routing::{get, post},
        Json, Router,
    };
    use futures_util::stream;
    use loxa_core::gateway::{EngineTarget, GatewayState, GenerationOutput};
    use serde_json::{json, Value};
    use std::{
        convert::Infallible,
        io::{self, Write},
        sync::{Arc, Mutex},
    };
    use tower::ServiceExt;
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::fmt::format::FmtSpan;

    use super::{apply, apply_with_source, DiagnosticRequestId, RequestIdSource};

    #[derive(Clone, Copy, Debug)]
    struct FailingRequestIdSource;

    impl RequestIdSource for FailingRequestIdSource {
        fn fill(&mut self, _bytes: &mut [u8; 16]) -> Result<(), ()> {
            Err(())
        }
    }

    #[derive(Clone, Default)]
    struct Capture(Arc<Mutex<Vec<u8>>>);

    struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for CaptureWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0
                .lock()
                .expect("capture poisoned")
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
        type Writer = CaptureWriter;

        fn make_writer(&'a self) -> Self::Writer {
            CaptureWriter(Arc::clone(&self.0))
        }
    }

    impl Capture {
        fn subscriber(&self) -> impl tracing::Subscriber + Send + Sync {
            tracing_subscriber::fmt()
                .with_ansi(false)
                .with_span_events(FmtSpan::FULL)
                .with_writer(self.clone())
                .finish()
        }

        fn text(&self) -> String {
            String::from_utf8(self.0.lock().expect("capture poisoned").clone())
                .expect("capture is UTF-8")
        }
    }

    fn is_request_id(value: &str) -> bool {
        value.len() == 32
            && value
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    }

    async fn known(Extension(request_id): Extension<DiagnosticRequestId>) -> impl IntoResponse {
        tracing::info!(result_class = "proxy_boundary", "proxy boundary");
        ([("x-request-id", "route-hostile")], request_id.0)
    }

    fn app() -> Router {
        apply(Router::new().route("/known/{id}", get(known)).route(
            "/events",
            get(|| async {
                Sse::new(stream::iter([
                    Ok::<_, Infallible>(Event::default().event("first").data("one")),
                    Ok::<_, Infallible>(Event::default().event("second").data("two")),
                ]))
            }),
        ))
    }

    #[tokio::test]
    async fn replaces_untrusted_inbound_request_id_and_records_only_safe_route_data() {
        let capture = Capture::default();
        let mut request = Request::builder()
            .uri("/known/PRIVATE_PATH?token=QUERY_SECRET")
            .header("x-request-id", "Bearer hostile")
            .header(header::AUTHORIZATION, "Bearer AUTH_SECRET")
            .body(Body::from("BODY_SECRET"))
            .unwrap();
        request
            .extensions_mut()
            .insert(tower_http::request_id::RequestId::new(
                "extension-hostile".parse().unwrap(),
            ));
        request
            .extensions_mut()
            .insert(DiagnosticRequestId("diagnostic-hostile".to_owned()));

        let response = app()
            .oneshot(request)
            .with_subscriber(capture.subscriber())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let response_id = response.headers()["x-request-id"]
            .to_str()
            .unwrap()
            .to_owned();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body_id = std::str::from_utf8(&body).unwrap();
        let captured = capture.text();

        assert!(is_request_id(&response_id));
        assert_eq!(response_id, body_id);
        assert!(captured.contains(&response_id));
        assert!(captured.contains("/known/{id}"));
        assert!(captured.contains("http.request.completed"));
        assert!(captured.contains("component=\"http\""));
        for forbidden in [
            "Bearer hostile",
            "extension-hostile",
            "diagnostic-hostile",
            "PRIVATE_PATH",
            "QUERY_SECRET",
            "AUTH_SECRET",
            "BODY_SECRET",
        ] {
            assert!(!captured.contains(forbidden), "captured {forbidden}");
        }
    }

    #[tokio::test]
    async fn assigns_an_id_without_an_inbound_header() {
        let response = app()
            .oneshot(
                Request::builder()
                    .uri("/known/1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(is_request_id(
            response.headers()["x-request-id"].to_str().unwrap()
        ));
    }

    fn failure_contract_router() -> Router {
        Router::new().route(
            "/generation-failure",
            get(|| async {
                (
                    StatusCode::IM_A_TEAPOT,
                    [("x-route-contract", "unchanged")],
                    "exact route body",
                )
            }),
        )
    }

    #[tokio::test]
    async fn request_id_generation_failure_preserves_the_route_response_without_correlation() {
        let baseline = failure_contract_router()
            .oneshot(
                Request::builder()
                    .uri("/generation-failure")
                    .header("x-request-id", "HOSTILE_INBOUND_ID")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let observed = apply_with_source(failure_contract_router(), FailingRequestIdSource)
            .oneshot(
                Request::builder()
                    .uri("/generation-failure")
                    .header("x-request-id", "HOSTILE_INBOUND_ID")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let (baseline_parts, baseline_body) = baseline.into_parts();
        let (observed_parts, observed_body) = observed.into_parts();
        assert_eq!(observed_parts.status, baseline_parts.status);
        assert_eq!(observed_parts.headers, baseline_parts.headers);
        assert!(!observed_parts.headers.contains_key("x-request-id"));
        assert_eq!(
            to_bytes(observed_body, usize::MAX).await.unwrap(),
            to_bytes(baseline_body, usize::MAX).await.unwrap()
        );
    }

    #[tokio::test]
    async fn records_unmatched_without_recording_the_raw_path() {
        let capture = Capture::default();
        let response = app()
            .oneshot(
                Request::builder()
                    .uri("/SECRET_UNMATCHED?credential=SECRET_QUERY")
                    .body(Body::empty())
                    .unwrap(),
            )
            .with_subscriber(capture.subscriber())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let captured = capture.text();
        assert!(captured.contains("unmatched"));
        assert!(!captured.contains("SECRET_UNMATCHED"));
        assert!(!captured.contains("SECRET_QUERY"));
    }

    #[tokio::test]
    async fn preserves_sse_items_and_order() {
        let response = app()
            .oneshot(
                Request::builder()
                    .uri("/events")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            body.as_ref(),
            b"event: first\ndata: one\n\nevent: second\ndata: two\n\n"
        );
    }

    async fn proxy(
        State(gateway): State<GatewayState>,
        Extension(request_id): Extension<DiagnosticRequestId>,
        Json(request): Json<Value>,
    ) -> impl IntoResponse {
        let prepared = gateway.prepare_generation(request).unwrap();
        tracing::info!(result_class = "proxy_boundary", "proxy boundary");
        let GenerationOutput::Json { status, body } = prepared.execute().await.unwrap() else {
            panic!("expected JSON proxy response")
        };
        (status, [("x-diagnostic-test-id", request_id.0)], Json(body))
    }

    #[tokio::test]
    async fn keeps_request_span_entered_across_gateway_proxy_execution() {
        let upstream_request_id = Arc::new(Mutex::new(None));
        let captured_upstream_request_id = Arc::clone(&upstream_request_id);
        let upstream = Router::new().route(
            "/v1/chat/completions",
            post(move |headers: HeaderMap| {
                let captured_upstream_request_id = Arc::clone(&captured_upstream_request_id);
                async move {
                    *captured_upstream_request_id
                        .lock()
                        .expect("upstream capture poisoned") = headers
                        .get("x-request-id")
                        .map(|value| value.as_bytes().to_vec());
                    Json(json!({"id":"same-public-json","model":"engine-model"}))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let upstream_task = tokio::spawn(async move {
            axum::serve(listener, upstream).await.unwrap();
        });
        let gateway = GatewayState::new("correlation-test");
        gateway.publish(EngineTarget {
            base_url: format!("http://{address}"),
            backend_alias: "engine-model".to_owned(),
            engine: "test".to_owned(),
            engine_version: "1".to_owned(),
            model_id: "test-model".to_owned(),
            profile: "test".to_owned(),
        });
        let app = apply(
            Router::new()
                .route("/proxy", post(proxy))
                .with_state(gateway),
        );
        let capture = Capture::default();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/proxy")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("x-request-id", "UNTRUSTED_PROXY_ID")
                    .body(Body::from(r#"{"model":"loxa"}"#))
                    .unwrap(),
            )
            .with_subscriber(capture.subscriber())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let response_id = response.headers()["x-request-id"]
            .to_str()
            .unwrap()
            .to_owned();
        let diagnostic_id = response.headers()["x-diagnostic-test-id"]
            .to_str()
            .unwrap()
            .to_owned();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        upstream_task.abort();

        assert_eq!(response_id, diagnostic_id);
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            json!({"id":"same-public-json","model":"loxa"})
        );
        assert_eq!(
            *upstream_request_id
                .lock()
                .expect("upstream capture poisoned"),
            None
        );
        let captured = capture.text();
        let proxy_line = captured
            .lines()
            .find(|line| line.contains("proxy boundary"))
            .expect("proxy-boundary event captured");
        assert!(proxy_line.contains(&response_id));
        assert!(!captured.contains("UNTRUSTED_PROXY_ID"));
    }
}
