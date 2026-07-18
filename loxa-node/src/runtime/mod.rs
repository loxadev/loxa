mod node_runtime;

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
pub(crate) use node_runtime::{
    DurableHealthMonitor, NodeOwnerGuard, NodeRuntime, NodeRuntimeParts,
};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;

const PUBLICATION_CLOSED: u8 = 0;
const PUBLICATION_OPEN: u8 = 1;
const PUBLICATION_SEALED: u8 = 2;

struct PublicationGateState {
    state: AtomicU8,
    durable_healthy: Option<Arc<AtomicBool>>,
}

#[derive(Clone)]
pub(crate) struct PublicationGate(Arc<PublicationGateState>);

impl Default for PublicationGate {
    fn default() -> Self {
        Self(Arc::new(PublicationGateState {
            state: AtomicU8::new(PUBLICATION_CLOSED),
            durable_healthy: None,
        }))
    }
}

impl PublicationGate {
    pub(crate) fn with_durable_health(durable_healthy: Arc<AtomicBool>) -> Self {
        Self(Arc::new(PublicationGateState {
            state: AtomicU8::new(PUBLICATION_CLOSED),
            durable_healthy: Some(durable_healthy),
        }))
    }

    pub(crate) fn open(&self) -> bool {
        self.0
            .state
            .compare_exchange(
                PUBLICATION_CLOSED,
                PUBLICATION_OPEN,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    pub(crate) fn close(&self) {
        self.0.state.store(PUBLICATION_SEALED, Ordering::Release);
    }

    pub(crate) fn is_open(&self) -> bool {
        self.0.state.load(Ordering::Acquire) == PUBLICATION_OPEN
            && self
                .0
                .durable_healthy
                .as_ref()
                .is_none_or(|healthy| healthy.load(Ordering::Acquire))
    }

    pub(crate) fn protect(&self, router: Router) -> Router {
        router.layer(middleware::from_fn_with_state(
            self.clone(),
            enforce_publication_gate,
        ))
    }
}

async fn enforce_publication_gate(
    State(gate): State<PublicationGate>,
    request: Request,
    next: Next,
) -> Response {
    if gate.is_open() {
        return next.run(request).await;
    }
    if request.uri().path().starts_with("/loxa/v2/") {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(loxa_protocol::v2::V2ControlErrorBody {
                code: loxa_protocol::v2::V2ControlErrorCode::DurableStateUnavailable,
                message: "Durable control state is unavailable.".into(),
            }),
        )
            .into_response();
    }
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(loxa_protocol::v1::ControlErrorBody {
            code: loxa_protocol::v1::ControlErrorCode::NodeStopping,
            message: "node is stopping".into(),
        }),
    )
        .into_response()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct EffectiveCapabilityInputs {
    pub(crate) downloader_owner: bool,
    pub(crate) slot_load_support: bool,
    pub(crate) slot_unload_support: bool,
    pub(crate) cancellation_authority: bool,
    pub(crate) durable_writer_healthy: bool,
    pub(crate) subscription_healthy: bool,
}

pub(crate) fn effective_capabilities(
    inputs: EffectiveCapabilityInputs,
) -> loxa_protocol::v2::V2NodeCapabilities {
    loxa_protocol::v2::V2NodeCapabilities {
        model_download: inputs.downloader_owner,
        slot_load: inputs.slot_load_support,
        slot_unload: inputs.slot_unload_support,
        operation_cancel: inputs.cancellation_authority,
        operation_stream: inputs.durable_writer_healthy && inputs.subscription_healthy,
    }
}
