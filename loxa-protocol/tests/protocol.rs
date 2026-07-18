use loxa_protocol::v1::{
    ControlErrorBody, ControlErrorCode, NodeIdentityProofResponse, NodeStatus,
    CONTROL_PROTOCOL_VERSION,
};
use loxa_protocol::v2::{
    DecimalU64, OperationId, StreamEpoch, V2ControlErrorBody, V2ControlErrorCode, V2ControlEvent,
    V2NodeCollection, V2OperationAccepted, V2OperationCollection, V2OperationEnvelope,
    V2OperationProgress, V2PublicError, V2ReconnectSnapshot, V2Slot, V2SlotCollection,
};
use loxa_protocol::{NodeId, NodeInstanceId};
use std::str::FromStr;

#[test]
fn identifiers_accept_only_canonical_non_nil_uuid_v4_text() {
    let canonical = "123e4567-e89b-42d3-a456-426614174000";
    let node_id = NodeId::from_str(canonical).unwrap();
    let instance_id = NodeInstanceId::from_str(canonical).unwrap();
    assert_eq!(node_id.to_string(), canonical);
    assert_eq!(instance_id.to_string(), canonical);
    assert_eq!(
        serde_json::to_string(&node_id).unwrap(),
        format!("\"{canonical}\"")
    );
    assert_eq!(
        serde_json::from_str::<NodeId>(&format!("\"{canonical}\"")).unwrap(),
        node_id
    );

    for invalid in [
        "123E4567-E89B-42D3-A456-426614174000",
        "{123e4567-e89b-42d3-a456-426614174000}",
        "urn:uuid:123e4567-e89b-42d3-a456-426614174000",
        " 123e4567-e89b-42d3-a456-426614174000",
        "123e4567e89b42d3a456426614174000",
        "00000000-0000-0000-0000-000000000000",
        "123e4567-e89b-12d3-a456-426614174000",
        "123e4567-e89b-42d3-7456-426614174000",
        "123e4567-e89b-42d3-a456-426614174000x",
        "123e4567-e89b-42d3-a456-426614174000-extra-overlong",
    ] {
        assert!(NodeId::from_str(invalid).is_err(), "accepted {invalid}");
        assert!(
            NodeInstanceId::from_str(invalid).is_err(),
            "accepted {invalid}"
        );
        assert!(serde_json::from_str::<NodeId>(&format!("\"{invalid}\"")).is_err());
    }
    for version in b"012356789abcdef" {
        let mut invalid = canonical.as_bytes().to_vec();
        invalid[14] = *version;
        let invalid = std::str::from_utf8(&invalid).unwrap();
        assert!(
            NodeId::from_str(invalid).is_err(),
            "accepted version {version}"
        );
    }
    assert!(serde_json::from_str::<NodeId>("7").is_err());
}

#[test]
fn identity_generation_is_v4_and_distinct() {
    let first = NodeId::new_v4();
    let second = NodeId::new_v4();
    let instance = NodeInstanceId::new_v4();
    assert_ne!(first, second);
    assert!(NodeId::from_str(&first.to_string()).is_ok());
    assert!(NodeInstanceId::from_str(&instance.to_string()).is_ok());
}

#[test]
fn proof_json_is_exact_and_keeps_opaque_v1_identities() {
    let response = NodeIdentityProofResponse::new(
        CONTROL_PROTOCOL_VERSION,
        "legacy opaque node".into(),
        "legacy/runtime#identity".into(),
        NodeStatus::Ready,
        "00".repeat(32),
    )
    .unwrap();
    assert_eq!(
        serde_json::to_string(&response).unwrap(),
        format!(
            "{{\"protocol_version\":1,\"node_id\":\"legacy opaque node\",\"runtime_identity\":\"legacy/runtime#identity\",\"status\":\"ready\",\"challenge_proof\":\"{}\"}}",
            "00".repeat(32)
        )
    );
    assert!(
        serde_json::from_value::<NodeIdentityProofResponse>(serde_json::json!({
            "protocol_version": 1,
            "node_id": "not-a-uuid",
            "runtime_identity": "also-not-a-uuid",
            "status": "ready",
            "challenge_proof": "00".repeat(32),
            "extra": true,
        }))
        .is_err()
    );
}

#[test]
fn error_codes_and_body_shape_are_exact() {
    let expected = [
        (ControlErrorCode::OperationConflict, "operation_conflict"),
        (ControlErrorCode::OperationNotFound, "operation_not_found"),
        (ControlErrorCode::OperationTerminal, "operation_terminal"),
        (ControlErrorCode::NodeStopping, "node_stopping"),
        (
            ControlErrorCode::CancellationNotSafe,
            "cancellation_not_safe",
        ),
        (ControlErrorCode::ModelUnavailable, "model_unavailable"),
        (
            ControlErrorCode::UnsupportedMediaType,
            "unsupported_media_type",
        ),
        (ControlErrorCode::UnknownModel, "unknown_model"),
    ];
    for (code, wire) in expected {
        assert_eq!(code.to_string(), wire);
        assert_eq!(serde_json::to_string(&code).unwrap(), format!("\"{wire}\""));
    }
    assert!(serde_json::from_str::<ControlErrorCode>("\"other\"").is_err());
    let body = ControlErrorBody {
        code: ControlErrorCode::OperationConflict,
        message: "busy".into(),
    };
    assert_eq!(
        serde_json::to_string(&body).unwrap(),
        r#"{"code":"operation_conflict","message":"busy"}"#
    );
    assert!(serde_json::from_str::<ControlErrorBody>(
        r#"{"code":"operation_conflict","message":"busy","extra":true}"#
    )
    .is_err());
    assert!(serde_json::from_str::<ControlErrorBody>(
        r#"{"code":"not_a_real_code","message":"busy"}"#
    )
    .is_err());
}

#[test]
fn v2_identifiers_require_canonical_uuid_v4_and_remain_distinct() {
    let operation: OperationId = "123e4567-e89b-42d3-9456-426614174003".parse().unwrap();
    assert_eq!(
        operation.to_string(),
        "123e4567-e89b-42d3-9456-426614174003"
    );
    assert!("123E4567-E89B-42D3-9456-426614174003"
        .parse::<OperationId>()
        .is_err());
    assert!("123e4567-e89b-12d3-9456-426614174003"
        .parse::<OperationId>()
        .is_err());
}

#[test]
fn decimal_u64_rejects_noncanonical_or_overflowing_values() {
    for value in ["", "00", "+1", " 1", "1.0", "18446744073709551616"] {
        assert!(serde_json::from_str::<DecimalU64>(&format!("\"{value}\"")).is_err());
    }
    assert_eq!(
        serde_json::to_string(&DecimalU64::new(42)).unwrap(),
        "\"42\""
    );
}

fn operation_accepted_fixture() -> V2OperationAccepted {
    V2OperationAccepted {
        epoch: "123e4567-e89b-42d3-b456-426614174005"
            .parse::<StreamEpoch>()
            .unwrap(),
        operation_id: "123e4567-e89b-42d3-9456-426614174003"
            .parse::<OperationId>()
            .unwrap(),
        revision: DecimalU64::new(10),
    }
}

fn slice4_operation_accepted_fixture() -> V2OperationAccepted {
    V2OperationAccepted {
        operation_id: "11111111-1111-4111-8111-111111111111"
            .parse::<OperationId>()
            .unwrap(),
        epoch: "22222222-2222-4222-8222-222222222222"
            .parse::<StreamEpoch>()
            .unwrap(),
        revision: DecimalU64::new(7),
    }
}

#[test]
fn slice4_keeps_operation_acceptance_and_overload_wire_exact() {
    let accepted = slice4_operation_accepted_fixture();
    assert_eq!(
        serde_json::to_value(accepted).unwrap(),
        serde_json::json!({
            "operation_id": "11111111-1111-4111-8111-111111111111",
            "epoch": "22222222-2222-4222-8222-222222222222",
            "revision": "7"
        })
    );
    assert_eq!(
        serde_json::to_value(V2PublicError {
            code: V2ControlErrorCode::OperationConflict,
            message: "A conflicting operation is active.".into(),
        })
        .unwrap(),
        serde_json::json!({
            "code": "operation_conflict",
            "message": "A conflicting operation is active."
        })
    );
}

#[test]
fn slice4_rejects_new_wire_enums_and_fields() {
    let mut verify = operation_json(NODE_ID, "download", "running");
    verify["kind"] = serde_json::json!("verify");
    assert!(serde_json::from_value::<loxa_protocol::v2::V2Operation>(verify).is_err());

    assert!(
        serde_json::from_value::<V2ControlErrorBody>(serde_json::json!({
            "code": "operation_overloaded",
            "message": "The operation queue is full."
        }))
        .is_err()
    );

    let mut attachment = serde_json::to_value(slice4_operation_accepted_fixture()).unwrap();
    attachment["attachment"] = serde_json::json!("download-worker-1");
    assert!(serde_json::from_value::<V2OperationAccepted>(attachment).is_err());

    for field in ["phase", "retry"] {
        let mut operation = operation_json(NODE_ID, "download", "running");
        operation[field] = serde_json::json!(true);
        assert!(
            serde_json::from_value::<loxa_protocol::v2::V2Operation>(operation).is_err(),
            "accepted unexpected {field} field"
        );
    }
}

#[test]
fn v2_operation_acceptance_has_exact_keys_and_string_counters() {
    let value = serde_json::to_value(operation_accepted_fixture()).unwrap();
    assert_eq!(
        value,
        serde_json::json!({
            "epoch": "123e4567-e89b-42d3-b456-426614174005",
            "operation_id": "123e4567-e89b-42d3-9456-426614174003",
            "revision": "10"
        })
    );
}

#[test]
fn v2_contracts_reject_unknown_missing_duplicate_and_numeric_counter_fields() {
    assert!(serde_json::from_str::<V2OperationAccepted>(
        r#"{"epoch":"123e4567-e89b-42d3-b456-426614174005","operation_id":"123e4567-e89b-42d3-9456-426614174003","revision":10}"#
    )
    .is_err());
    assert!(serde_json::from_slice::<V2OperationAccepted>(br#"{"epoch":"123e4567-e89b-42d3-b456-426614174005","epoch":"123e4567-e89b-42d3-b456-426614174005","operation_id":"123e4567-e89b-42d3-9456-426614174003","revision":"10"}"#).is_err());
}

#[test]
fn v2_event_rejects_an_envelope_without_a_committed_record() {
    assert!(serde_json::from_str::<V2ControlEvent>(
        r#"{
            "schema_version":2,
            "event_id":"123e4567-e89b-42d3-a456-426614174004",
            "epoch":"123e4567-e89b-42d3-b456-426614174005",
            "sequence":"11",
            "revision":"11",
            "committed_at_unix_ms":"1784246400500",
            "entity":"operation",
            "entity_id":"123e4567-e89b-42d3-9456-426614174003",
            "node_id":"123e4567-e89b-42d3-a456-426614174000",
            "node_instance_id":null,
            "slot_id":null,
            "operation_id":null,
            "node":null,
            "slot":null,
            "operation":null
        }"#
    )
    .is_err());
}

#[test]
fn v2_nullable_fields_must_be_present_even_when_null() {
    assert!(serde_json::from_str::<V2Slot>(
        r#"{
            "slot_id":"123e4567-e89b-42d3-8456-426614174002",
            "node_id":"123e4567-e89b-42d3-a456-426614174000",
            "name":"default",
            "status":"ready",
            "model_id":"gemma-3-4b-it-q4",
            "error":null
        }"#
    )
    .is_err());
}

const NODE_ID: &str = "123e4567-e89b-42d3-a456-426614174000";
const OTHER_NODE_ID: &str = "123e4567-e89b-42d3-a456-426614174010";
const INSTANCE_ID: &str = "123e4567-e89b-42d3-b456-426614174001";
const SLOT_ID: &str = "123e4567-e89b-42d3-8456-426614174002";
const OPERATION_ID: &str = "123e4567-e89b-42d3-9456-426614174003";
const OTHER_OPERATION_ID: &str = "123e4567-e89b-42d3-9456-426614174013";
const EPOCH: &str = "123e4567-e89b-42d3-b456-426614174005";

fn node_json(node_id: &str) -> serde_json::Value {
    serde_json::json!({
        "node_id": node_id,
        "node_instance_id": INSTANCE_ID,
        "control_endpoint": "http://127.0.0.1:8080",
        "status": "running",
        "slot_capacity": 1,
        "capabilities": {
            "model_download": true,
            "slot_load": true,
            "slot_unload": true,
            "operation_cancel": true,
            "operation_stream": true
        }
    })
}

fn slot_json(node_id: &str, status: &str, operation_id: Option<&str>) -> serde_json::Value {
    let (model_id, error) = match status {
        "loading" => (serde_json::Value::Null, serde_json::Value::Null),
        "ready" | "unloading" => (
            serde_json::json!("gemma-3-4b-it-q4"),
            serde_json::Value::Null,
        ),
        _ => (serde_json::Value::Null, serde_json::Value::Null),
    };
    serde_json::json!({
        "slot_id": SLOT_ID,
        "node_id": node_id,
        "name": "default",
        "status": status,
        "model_id": model_id,
        "operation_id": operation_id,
        "error": error
    })
}

fn operation_json(node_id: &str, kind: &str, status: &str) -> serde_json::Value {
    serde_json::json!({
        "operation_id": OPERATION_ID,
        "node_id": node_id,
        "kind": kind,
        "status": status,
        "slot_id": if kind == "download" { serde_json::Value::Null } else { serde_json::json!(SLOT_ID) },
        "model_id": if kind == "unload" { serde_json::Value::Null } else { serde_json::json!("gemma-3-4b-it-q4") },
        "progress": serde_json::Value::Null,
        "error": serde_json::Value::Null,
        "created_revision": "10",
        "updated_revision": "11",
        "created_at_unix_ms": "1784246400000",
        "updated_at_unix_ms": "1784246400500"
    })
}

fn event_json() -> serde_json::Value {
    serde_json::json!({
        "schema_version": 2,
        "event_id": "123e4567-e89b-42d3-a456-426614174004",
        "epoch": EPOCH,
        "sequence": "11",
        "revision": "11",
        "committed_at_unix_ms": "1784246400500",
        "entity": "operation",
        "entity_id": OPERATION_ID,
        "node_id": NODE_ID,
        "node_instance_id": INSTANCE_ID,
        "slot_id": SLOT_ID,
        "operation_id": OPERATION_ID,
        "node": null,
        "slot": slot_json(NODE_ID, "loading", Some(OPERATION_ID)),
        "operation": operation_json(NODE_ID, "load", "running")
    })
}

fn event_json_at(event_id: &str, sequence: &str, revision: &str) -> serde_json::Value {
    let mut event = event_json();
    event["event_id"] = serde_json::json!(event_id);
    event["sequence"] = serde_json::json!(sequence);
    event["revision"] = serde_json::json!(revision);
    event["operation"]["updated_revision"] = serde_json::json!(revision);
    event
}

fn terminal_operation_json(
    operation_id: &str,
    created_revision: u64,
    updated_revision: u64,
    updated_at_unix_ms: u64,
) -> serde_json::Value {
    let mut operation = operation_json(NODE_ID, "load", "succeeded");
    operation["operation_id"] = serde_json::json!(operation_id);
    operation["created_revision"] = serde_json::json!(created_revision.to_string());
    operation["updated_revision"] = serde_json::json!(updated_revision.to_string());
    operation["created_at_unix_ms"] = serde_json::json!(updated_at_unix_ms.to_string());
    operation["updated_at_unix_ms"] = serde_json::json!(updated_at_unix_ms.to_string());
    operation
}

fn operation_id_at(index: u64) -> String {
    format!("123e4567-e89b-42d3-9456-{index:012x}")
}

#[test]
fn v2_serving_collections_and_operation_envelopes_reject_zero_revisions() {
    let fixtures = [
        serde_json::json!({
            "schema_version": 2,
            "epoch": EPOCH,
            "revision": "0",
            "generated_at_unix_ms": "1784246400600",
            "nodes": [node_json(NODE_ID)]
        }),
        serde_json::json!({
            "schema_version": 2,
            "epoch": EPOCH,
            "revision": "0",
            "generated_at_unix_ms": "1784246400600",
            "node_id": NODE_ID,
            "slots": [slot_json(NODE_ID, "ready", None)]
        }),
        serde_json::json!({
            "schema_version": 2,
            "epoch": EPOCH,
            "revision": "0",
            "generated_at_unix_ms": "1784246400600",
            "operations": []
        }),
        serde_json::json!({
            "schema_version": 2,
            "epoch": EPOCH,
            "revision": "0",
            "generated_at_unix_ms": "1784246400500",
            "operation": operation_json(NODE_ID, "load", "running")
        }),
    ];

    assert!(serde_json::from_value::<V2NodeCollection>(fixtures[0].clone()).is_err());
    assert!(serde_json::from_value::<V2SlotCollection>(fixtures[1].clone()).is_err());
    assert!(serde_json::from_value::<V2OperationCollection>(fixtures[2].clone()).is_err());
    assert!(serde_json::from_value::<V2OperationEnvelope>(fixtures[3].clone()).is_err());
}

#[test]
fn v2_operation_collection_enforces_bound_uniqueness_and_canonical_order() {
    let operations: Vec<_> = (1..=257)
        .map(|index| {
            terminal_operation_json(
                &operation_id_at(index),
                index,
                index,
                1_784_246_400_000 + index,
            )
        })
        .collect();
    let at_bound = serde_json::json!({
        "schema_version": 2,
        "epoch": EPOCH,
        "revision": "300",
        "generated_at_unix_ms": "1784246401000",
        "operations": operations[..256]
    });
    assert!(serde_json::from_value::<V2OperationCollection>(at_bound).is_ok());

    let over_bound = serde_json::json!({
        "schema_version": 2,
        "epoch": EPOCH,
        "revision": "300",
        "generated_at_unix_ms": "1784246401000",
        "operations": operations
    });
    assert!(serde_json::from_value::<V2OperationCollection>(over_bound).is_err());

    let first = terminal_operation_json(&operation_id_at(1), 10, 10, 100);
    let duplicate = serde_json::json!({
        "schema_version": 2,
        "epoch": EPOCH,
        "revision": "12",
        "generated_at_unix_ms": "200",
        "operations": [first.clone(), first]
    });
    assert!(serde_json::from_value::<V2OperationCollection>(duplicate).is_err());

    let duplicate_id_at_different_revisions = serde_json::json!({
        "schema_version": 2,
        "epoch": EPOCH,
        "revision": "12",
        "generated_at_unix_ms": "200",
        "operations": [
            terminal_operation_json(&operation_id_at(1), 10, 10, 100),
            terminal_operation_json(&operation_id_at(1), 11, 11, 101)
        ]
    });
    assert!(
        serde_json::from_value::<V2OperationCollection>(duplicate_id_at_different_revisions)
            .is_err()
    );

    let noncanonical = serde_json::json!({
        "schema_version": 2,
        "epoch": EPOCH,
        "revision": "12",
        "generated_at_unix_ms": "200",
        "operations": [
            terminal_operation_json(&operation_id_at(2), 11, 11, 101),
            terminal_operation_json(&operation_id_at(1), 10, 10, 100)
        ]
    });
    assert!(serde_json::from_value::<V2OperationCollection>(noncanonical).is_err());

    let same_revision_wrong_id_order = serde_json::json!({
        "schema_version": 2,
        "epoch": EPOCH,
        "revision": "12",
        "generated_at_unix_ms": "200",
        "operations": [
            terminal_operation_json(&operation_id_at(2), 10, 10, 100),
            terminal_operation_json(&operation_id_at(1), 10, 10, 100)
        ]
    });
    assert!(serde_json::from_value::<V2OperationCollection>(same_revision_wrong_id_order).is_err());

    let mut revision_ahead = serde_json::json!({
        "schema_version": 2,
        "epoch": EPOCH,
        "revision": "12",
        "generated_at_unix_ms": "200",
        "operations": [terminal_operation_json(&operation_id_at(1), 13, 13, 100)]
    });
    assert!(serde_json::from_value::<V2OperationCollection>(revision_ahead.clone()).is_err());

    revision_ahead["revision"] = serde_json::json!("13");
    revision_ahead["operations"][0]["updated_at_unix_ms"] = serde_json::json!("201");
    assert!(serde_json::from_value::<V2OperationCollection>(revision_ahead).is_err());
}

#[test]
fn v2_operations_and_acceptance_reject_zero_committed_revisions() {
    let mut zero_operation = operation_json(NODE_ID, "load", "running");
    zero_operation["created_revision"] = serde_json::json!("0");
    zero_operation["updated_revision"] = serde_json::json!("0");
    assert!(
        serde_json::from_value::<loxa_protocol::v2::V2Operation>(zero_operation.clone()).is_err()
    );

    let zero_nested = serde_json::json!({
        "schema_version": 2,
        "epoch": EPOCH,
        "revision": "11",
        "generated_at_unix_ms": "1784246400500",
        "operation": zero_operation
    });
    assert!(serde_json::from_value::<V2OperationEnvelope>(zero_nested).is_err());

    let mut operation: loxa_protocol::v2::V2Operation =
        serde_json::from_value(operation_json(NODE_ID, "load", "running")).unwrap();
    operation.created_revision = DecimalU64::new(0);
    operation.updated_revision = DecimalU64::new(0);
    assert!(serde_json::to_value(operation).is_err());

    let zero_accepted = serde_json::json!({
        "epoch": EPOCH,
        "operation_id": OPERATION_ID,
        "revision": "0"
    });
    assert!(serde_json::from_value::<V2OperationAccepted>(zero_accepted).is_err());

    let mut accepted = operation_accepted_fixture();
    accepted.revision = DecimalU64::new(0);
    assert!(serde_json::to_value(accepted).is_err());
}

#[test]
fn v2_operation_envelopes_contain_child_revision_and_observation_time() {
    let mut revision_ahead = serde_json::json!({
        "schema_version": 2,
        "epoch": EPOCH,
        "revision": "11",
        "generated_at_unix_ms": "1784246400600",
        "operation": operation_json(NODE_ID, "load", "running")
    });
    revision_ahead["operation"]["updated_revision"] = serde_json::json!("12");
    assert!(serde_json::from_value::<V2OperationEnvelope>(revision_ahead).is_err());

    let mut time_ahead = serde_json::json!({
        "schema_version": 2,
        "epoch": EPOCH,
        "revision": "11",
        "generated_at_unix_ms": "1784246400500",
        "operation": operation_json(NODE_ID, "load", "running")
    });
    time_ahead["operation"]["updated_at_unix_ms"] = serde_json::json!("1784246400501");
    assert!(serde_json::from_value::<V2OperationEnvelope>(time_ahead).is_err());
}

#[test]
fn v2_events_contain_embedded_operation_revision_and_observation_time() {
    let mut revision_ahead = event_json();
    revision_ahead["operation"]["updated_revision"] = serde_json::json!("12");
    assert!(serde_json::from_value::<V2ControlEvent>(revision_ahead).is_err());

    let mut time_ahead = event_json();
    time_ahead["committed_at_unix_ms"] = serde_json::json!("1784246400499");
    assert!(serde_json::from_value::<V2ControlEvent>(time_ahead).is_err());
}

#[test]
fn v2_reconnect_snapshot_contains_bounded_canonical_children() {
    let base = serde_json::json!({
        "schema_version": 2,
        "epoch": EPOCH,
        "revision": "300",
        "generated_at_unix_ms": "1784246401000",
        "stream": {"epoch": EPOCH, "cursor": "300", "cursor_gap": false},
        "nodes": [node_json(NODE_ID)],
        "slots": [slot_json(NODE_ID, "ready", None)],
        "operations": [],
        "events": []
    });

    let mut over_bound = base.clone();
    over_bound["operations"] = serde_json::Value::Array(
        (1..=257)
            .map(|index| {
                terminal_operation_json(
                    &operation_id_at(index),
                    index,
                    index,
                    1_784_246_400_000 + index,
                )
            })
            .collect(),
    );
    assert!(serde_json::from_value::<V2ReconnectSnapshot>(over_bound).is_err());

    let mut noncanonical = base.clone();
    noncanonical["operations"] = serde_json::json!([
        terminal_operation_json(&operation_id_at(2), 11, 11, 101),
        terminal_operation_json(&operation_id_at(1), 10, 10, 100)
    ]);
    assert!(serde_json::from_value::<V2ReconnectSnapshot>(noncanonical).is_err());

    let mut revision_ahead = base.clone();
    revision_ahead["operations"] =
        serde_json::json!([terminal_operation_json(&operation_id_at(1), 301, 301, 100)]);
    assert!(serde_json::from_value::<V2ReconnectSnapshot>(revision_ahead).is_err());

    let mut time_ahead = base.clone();
    time_ahead["operations"] = serde_json::json!([terminal_operation_json(
        &operation_id_at(1),
        1,
        1,
        1_784_246_401_001
    )]);
    assert!(serde_json::from_value::<V2ReconnectSnapshot>(time_ahead).is_err());

    let mut event_time_ahead = base;
    event_time_ahead["events"] = serde_json::json!([event_json_at(
        "123e4567-e89b-42d3-a456-426614174004",
        "300",
        "300"
    )]);
    event_time_ahead["events"][0]["committed_at_unix_ms"] = serde_json::json!("1784246401001");
    event_time_ahead["events"][0]["operation"]["updated_at_unix_ms"] =
        serde_json::json!("1784246401001");
    assert!(serde_json::from_value::<V2ReconnectSnapshot>(event_time_ahead).is_err());
}

#[test]
fn v2_invalid_public_values_cannot_be_serialized() {
    let mut node: loxa_protocol::v2::V2Node = serde_json::from_value(node_json(NODE_ID)).unwrap();
    node.slot_capacity = 2;
    assert!(serde_json::to_value(&node).is_err());

    let mut node: loxa_protocol::v2::V2Node = serde_json::from_value(node_json(NODE_ID)).unwrap();
    node.control_endpoint = "https://example.com".into();
    assert!(serde_json::to_value(&node).is_err());

    let mut slot: V2Slot = serde_json::from_value(slot_json(NODE_ID, "ready", None)).unwrap();
    slot.name = "other".into();
    assert!(serde_json::to_value(&slot).is_err());

    let mut operation: loxa_protocol::v2::V2Operation =
        serde_json::from_value(operation_json(NODE_ID, "load", "running")).unwrap();
    operation.status = loxa_protocol::v2::V2OperationStatus::Failed;
    assert!(serde_json::to_value(&operation).is_err());

    let mut collection: V2NodeCollection = serde_json::from_value(serde_json::json!({
        "schema_version": 2,
        "epoch": EPOCH,
        "revision": "12",
        "generated_at_unix_ms": "1784246400600",
        "nodes": [node_json(NODE_ID)]
    }))
    .unwrap();
    collection.schema_version = 3;
    assert!(serde_json::to_value(&collection).is_err());

    let mut error: V2ControlErrorBody = serde_json::from_value(serde_json::json!({
        "code": "operation_conflict",
        "message": "A conflicting operation is active."
    }))
    .unwrap();
    error.message = "\n".into();
    assert!(serde_json::to_value(&error).is_err());
}

#[test]
fn v2_collections_and_reconnect_snapshot_enforce_capacity_one_and_correlation() {
    let node_collection = serde_json::json!({
        "schema_version": 2,
        "epoch": EPOCH,
        "revision": "12",
        "generated_at_unix_ms": "1784246400600",
        "nodes": []
    });
    assert!(serde_json::from_value::<V2NodeCollection>(node_collection).is_err());

    let slot_collection = serde_json::json!({
        "schema_version": 2,
        "epoch": EPOCH,
        "revision": "12",
        "generated_at_unix_ms": "1784246400600",
        "node_id": NODE_ID,
        "slots": [slot_json(OTHER_NODE_ID, "ready", None)]
    });
    assert!(serde_json::from_value::<V2SlotCollection>(slot_collection).is_err());

    let operation_collection = serde_json::json!({
        "schema_version": 2,
        "epoch": EPOCH,
        "revision": "12",
        "generated_at_unix_ms": "1784246400600",
        "operations": [
            operation_json(NODE_ID, "load", "running"),
            operation_json(OTHER_NODE_ID, "load", "running")
        ]
    });
    let mut operation_collection = operation_collection;
    operation_collection["operations"][1]["operation_id"] =
        serde_json::json!("123e4567-e89b-42d3-9456-426614174013");
    assert!(serde_json::from_value::<V2OperationCollection>(operation_collection).is_err());

    let snapshot = serde_json::json!({
        "schema_version": 2,
        "epoch": EPOCH,
        "revision": "11",
        "generated_at_unix_ms": "1784246400600",
        "stream": {"epoch": EPOCH, "cursor": "12", "cursor_gap": false},
        "nodes": [node_json(NODE_ID)],
        "slots": [slot_json(NODE_ID, "ready", None)],
        "operations": [],
        "events": []
    });
    assert!(serde_json::from_value::<V2ReconnectSnapshot>(snapshot).is_err());

    let snapshot = serde_json::json!({
        "schema_version": 2,
        "epoch": EPOCH,
        "revision": "12",
        "generated_at_unix_ms": "1784246400600",
        "stream": {"epoch": EPOCH, "cursor": "12", "cursor_gap": false},
        "nodes": [node_json(NODE_ID)],
        "slots": [slot_json(NODE_ID, "loading", Some(OPERATION_ID))],
        "operations": [operation_json(OTHER_NODE_ID, "load", "running")],
        "events": []
    });
    assert!(serde_json::from_value::<V2ReconnectSnapshot>(snapshot).is_err());
}

#[test]
fn slice4_keeps_concurrent_download_snapshots_and_unload_model_evidence_exact() {
    let load_id = "11111111-1111-4111-8111-111111111111";
    let first_download_id = "22222222-2222-4222-8222-222222222222";
    let second_download_id = "33333333-3333-4333-8333-333333333333";

    let mut load = operation_json(NODE_ID, "load", "running");
    load["operation_id"] = serde_json::json!(load_id);
    load["created_revision"] = serde_json::json!("10");
    load["updated_revision"] = serde_json::json!("10");
    let mut first_download = operation_json(NODE_ID, "download", "running");
    first_download["operation_id"] = serde_json::json!(first_download_id);
    first_download["created_revision"] = serde_json::json!("11");
    let mut second_download = operation_json(NODE_ID, "download", "queued");
    second_download["operation_id"] = serde_json::json!(second_download_id);
    second_download["created_revision"] = serde_json::json!("12");
    second_download["updated_revision"] = serde_json::json!("12");

    let snapshot_json = serde_json::json!({
        "schema_version": 2,
        "epoch": EPOCH,
        "revision": "12",
        "generated_at_unix_ms": "1784246400600",
        "stream": {"epoch": EPOCH, "cursor": "12", "cursor_gap": false},
        "nodes": [node_json(NODE_ID)],
        "slots": [slot_json(NODE_ID, "loading", Some(load_id))],
        "operations": [load.clone(), first_download.clone(), second_download.clone()],
        "events": []
    });
    let snapshot: V2ReconnectSnapshot = serde_json::from_value(snapshot_json.clone()).unwrap();
    assert_eq!(serde_json::to_value(&snapshot).unwrap(), snapshot_json);
    assert_eq!(
        snapshot
            .operations
            .iter()
            .map(|operation| operation.slot_id)
            .collect::<Vec<_>>(),
        vec![Some(SLOT_ID.parse().unwrap()), None, None]
    );

    let mut two_lifecycle = snapshot_json.clone();
    let mut unload = operation_json(NODE_ID, "unload", "queued");
    unload["operation_id"] = serde_json::json!(second_download_id);
    unload["created_revision"] = serde_json::json!("12");
    unload["updated_revision"] = serde_json::json!("12");
    two_lifecycle["operations"][2] = unload;
    assert!(serde_json::from_value::<V2ReconnectSnapshot>(two_lifecycle).is_err());

    let unload_id = "44444444-4444-4444-8444-444444444444";
    let mut unload = operation_json(NODE_ID, "unload", "running");
    unload["operation_id"] = serde_json::json!(unload_id);
    let unload_snapshot_json = serde_json::json!({
        "schema_version": 2,
        "epoch": EPOCH,
        "revision": "11",
        "generated_at_unix_ms": "1784246400600",
        "stream": {"epoch": EPOCH, "cursor": "11", "cursor_gap": false},
        "nodes": [node_json(NODE_ID)],
        "slots": [slot_json(NODE_ID, "unloading", Some(unload_id))],
        "operations": [unload],
        "events": []
    });
    let unload_snapshot: V2ReconnectSnapshot =
        serde_json::from_value(unload_snapshot_json.clone()).unwrap();
    assert_eq!(
        serde_json::to_value(&unload_snapshot).unwrap(),
        unload_snapshot_json
    );
    assert_eq!(
        unload_snapshot.slots[0].model_id.as_deref(),
        Some("gemma-3-4b-it-q4")
    );
    assert_eq!(unload_snapshot.operations[0].model_id, None);
}

#[test]
fn v2_progress_rejects_completed_bytes_above_a_known_total_standalone() {
    assert!(
        serde_json::from_value::<V2OperationProgress>(serde_json::json!({
            "completed_bytes": "2",
            "total_bytes": "1"
        }))
        .is_err()
    );
}

#[test]
fn v2_operation_failure_codes_match_the_operation_kind() {
    for (kind, wrong_code) in [
        ("download", "load_failed"),
        ("load", "unload_failed"),
        ("unload", "download_failed"),
    ] {
        let mut operation = operation_json(NODE_ID, kind, "failed");
        operation["error"] = serde_json::json!({"code": wrong_code, "message": "failed"});
        assert!(serde_json::from_value::<loxa_protocol::v2::V2Operation>(operation).is_err());
    }

    for (kind, interruption_code) in [
        ("download", "node_restarted_before_start"),
        ("load", "node_restarted"),
        ("unload", "cancellation_outcome_unknown"),
    ] {
        let mut operation = operation_json(NODE_ID, kind, "failed");
        operation["error"] =
            serde_json::json!({"code": interruption_code, "message": "interrupted"});
        assert!(serde_json::from_value::<loxa_protocol::v2::V2Operation>(operation).is_ok());
    }
}

#[test]
fn v2_collection_envelope_snapshot_and_error_fixtures_are_exact_and_strict() {
    let node_collection_json = serde_json::json!({
        "schema_version": 2,
        "epoch": EPOCH,
        "revision": "12",
        "generated_at_unix_ms": "1784246400600",
        "nodes": [node_json(NODE_ID)]
    });
    let node_collection: V2NodeCollection =
        serde_json::from_value(node_collection_json.clone()).unwrap();
    assert_eq!(
        serde_json::to_value(&node_collection).unwrap(),
        node_collection_json
    );

    let slot_collection_json = serde_json::json!({
        "schema_version": 2,
        "epoch": EPOCH,
        "revision": "12",
        "generated_at_unix_ms": "1784246400600",
        "node_id": NODE_ID,
        "slots": [slot_json(NODE_ID, "ready", None)]
    });
    let slot_collection: V2SlotCollection =
        serde_json::from_value(slot_collection_json.clone()).unwrap();
    assert_eq!(
        serde_json::to_value(&slot_collection).unwrap(),
        slot_collection_json
    );

    let operation_collection_json = serde_json::json!({
        "schema_version": 2,
        "epoch": EPOCH,
        "revision": "12",
        "generated_at_unix_ms": "1784246400600",
        "operations": []
    });
    let operation_collection: V2OperationCollection =
        serde_json::from_value(operation_collection_json.clone()).unwrap();
    assert_eq!(
        serde_json::to_value(&operation_collection).unwrap(),
        operation_collection_json
    );

    let operation_envelope_json = serde_json::json!({
        "schema_version": 2,
        "epoch": EPOCH,
        "revision": "11",
        "generated_at_unix_ms": "1784246400500",
        "operation": operation_json(NODE_ID, "load", "running")
    });
    let operation_envelope: V2OperationEnvelope =
        serde_json::from_value(operation_envelope_json.clone()).unwrap();
    assert_eq!(
        serde_json::to_value(&operation_envelope).unwrap(),
        operation_envelope_json
    );

    let snapshot_json = serde_json::json!({
        "schema_version": 2,
        "epoch": EPOCH,
        "revision": "12",
        "generated_at_unix_ms": "1784246400600",
        "stream": {"epoch": EPOCH, "cursor": "12", "cursor_gap": false},
        "nodes": [node_json(NODE_ID)],
        "slots": [slot_json(NODE_ID, "ready", None)],
        "operations": [],
        "events": []
    });
    let snapshot: V2ReconnectSnapshot = serde_json::from_value(snapshot_json.clone()).unwrap();
    assert_eq!(serde_json::to_value(&snapshot).unwrap(), snapshot_json);

    let control_error_json = serde_json::json!({
        "code": "operation_conflict",
        "message": "A conflicting operation is active."
    });
    let control_error: V2ControlErrorBody =
        serde_json::from_value(control_error_json.clone()).unwrap();
    assert_eq!(
        serde_json::to_value(&control_error).unwrap(),
        control_error_json
    );

    assert!(
        serde_json::from_value::<V2SlotCollection>(serde_json::json!({
            "schema_version": 2,
            "epoch": EPOCH,
            "revision": "12",
            "generated_at_unix_ms": "1784246400600",
            "node_id": NODE_ID,
            "slots": [slot_json(NODE_ID, "ready", None)],
            "extra": true
        }))
        .is_err()
    );
    assert!(serde_json::from_str::<V2OperationCollection>(
        &format!(r#"{{"schema_version":2,"epoch":"{EPOCH}","revision":"12","generated_at_unix_ms":"1","operations":[],"operations":[]}}"#)
    ).is_err());
    assert!(
        serde_json::from_value::<V2OperationEnvelope>(serde_json::json!({
            "schema_version": 2,
            "epoch": EPOCH,
            "revision": "11",
            "generated_at_unix_ms": "1784246400500"
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<V2ReconnectSnapshot>(serde_json::json!({
            "schema_version": 2,
            "epoch": EPOCH,
            "revision": "12",
            "generated_at_unix_ms": "1784246400600",
            "stream": {"epoch": EPOCH, "cursor": "12", "cursor_gap": false},
            "nodes": [node_json(NODE_ID)],
            "slots": [slot_json(NODE_ID, "ready", None)],
            "operations": []
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<V2ControlErrorBody>(serde_json::json!({
            "code": "not_real",
            "message": "failed"
        }))
        .is_err()
    );
    assert!(serde_json::from_str::<V2ControlErrorBody>(
        r#"{"code":"operation_conflict","code":"operation_conflict","message":"failed"}"#
    )
    .is_err());
    assert!(
        serde_json::from_value::<V2NodeCollection>(serde_json::json!({
            "schema_version": 1,
            "epoch": EPOCH,
            "revision": "12",
            "generated_at_unix_ms": "1784246400600",
            "nodes": [node_json(NODE_ID)]
        }))
        .is_err()
    );
    let mut invalid_capacity = node_json(NODE_ID);
    invalid_capacity["slot_capacity"] = serde_json::json!(2);
    assert!(serde_json::from_value::<loxa_protocol::v2::V2Node>(invalid_capacity).is_err());
}

#[test]
fn v2_reconnect_snapshot_enforces_inverse_active_slot_operation_correlation() {
    for status in ["queued", "running", "cancelling"] {
        let snapshot = serde_json::json!({
            "schema_version": 2,
            "epoch": EPOCH,
            "revision": "12",
            "generated_at_unix_ms": "1784246400600",
            "stream": {"epoch": EPOCH, "cursor": "12", "cursor_gap": false},
            "nodes": [node_json(NODE_ID)],
            "slots": [slot_json(NODE_ID, "ready", None)],
            "operations": [operation_json(NODE_ID, "load", status)],
            "events": []
        });
        assert!(
            serde_json::from_value::<V2ReconnectSnapshot>(snapshot).is_err(),
            "accepted active {status} load beside a ready slot"
        );
    }

    let mut second_operation = operation_json(NODE_ID, "unload", "running");
    second_operation["operation_id"] = serde_json::json!(OTHER_OPERATION_ID);
    let snapshot = serde_json::json!({
        "schema_version": 2,
        "epoch": EPOCH,
        "revision": "12",
        "generated_at_unix_ms": "1784246400600",
        "stream": {"epoch": EPOCH, "cursor": "12", "cursor_gap": false},
        "nodes": [node_json(NODE_ID)],
        "slots": [slot_json(NODE_ID, "loading", Some(OPERATION_ID))],
        "operations": [
            operation_json(NODE_ID, "load", "running"),
            second_operation
        ],
        "events": []
    });
    assert!(serde_json::from_value::<V2ReconnectSnapshot>(snapshot).is_err());

    let mut snapshot: V2ReconnectSnapshot = serde_json::from_value(serde_json::json!({
        "schema_version": 2,
        "epoch": EPOCH,
        "revision": "12",
        "generated_at_unix_ms": "1784246400600",
        "stream": {"epoch": EPOCH, "cursor": "12", "cursor_gap": false},
        "nodes": [node_json(NODE_ID)],
        "slots": [slot_json(NODE_ID, "ready", None)],
        "operations": [],
        "events": []
    }))
    .unwrap();
    snapshot
        .operations
        .push(serde_json::from_value(operation_json(NODE_ID, "unload", "running")).unwrap());
    assert!(serde_json::to_value(&snapshot).is_err());

    for (slot_status, kind, operation_status) in [
        ("loading", "load", "queued"),
        ("unloading", "unload", "cancelling"),
    ] {
        let snapshot = serde_json::json!({
            "schema_version": 2,
            "epoch": EPOCH,
            "revision": "12",
            "generated_at_unix_ms": "1784246400600",
            "stream": {"epoch": EPOCH, "cursor": "12", "cursor_gap": false},
            "nodes": [node_json(NODE_ID)],
            "slots": [slot_json(NODE_ID, slot_status, Some(OPERATION_ID))],
            "operations": [operation_json(NODE_ID, kind, operation_status)],
            "events": []
        });
        assert!(serde_json::from_value::<V2ReconnectSnapshot>(snapshot).is_ok());
    }

    let snapshot = serde_json::json!({
        "schema_version": 2,
        "epoch": EPOCH,
        "revision": "12",
        "generated_at_unix_ms": "1784246400600",
        "stream": {"epoch": EPOCH, "cursor": "12", "cursor_gap": false},
        "nodes": [node_json(NODE_ID)],
        "slots": [slot_json(NODE_ID, "ready", None)],
        "operations": [operation_json(NODE_ID, "load", "succeeded")],
        "events": []
    });
    assert!(serde_json::from_value::<V2ReconnectSnapshot>(snapshot).is_ok());
}

#[test]
fn v2_control_event_has_exact_strict_and_correlated_wire_contract() {
    let exact = event_json();
    let event: V2ControlEvent = serde_json::from_value(exact.clone()).unwrap();
    assert_eq!(serde_json::to_value(&event).unwrap(), exact);

    let mut unknown = event_json();
    unknown["extra"] = serde_json::json!(true);
    assert!(serde_json::from_value::<V2ControlEvent>(unknown).is_err());

    let mut missing = event_json();
    missing.as_object_mut().unwrap().remove("node");
    assert!(serde_json::from_value::<V2ControlEvent>(missing).is_err());

    let duplicate = serde_json::to_string(&event_json()).unwrap().replacen(
        "\"schema_version\":2",
        "\"schema_version\":2,\"schema_version\":2",
        1,
    );
    assert!(serde_json::from_str::<V2ControlEvent>(&duplicate).is_err());

    let mut missing_instance = event_json();
    missing_instance["node_instance_id"] = serde_json::Value::Null;
    assert!(serde_json::from_value::<V2ControlEvent>(missing_instance).is_err());

    let mut stale_slot = event_json();
    stale_slot["entity"] = serde_json::json!("node");
    stale_slot["entity_id"] = serde_json::json!(NODE_ID);
    stale_slot["node"] = node_json(NODE_ID);
    stale_slot["slot"] = serde_json::Value::Null;
    stale_slot["operation"] = serde_json::Value::Null;
    assert!(serde_json::from_value::<V2ControlEvent>(stale_slot).is_err());

    let mut event: V2ControlEvent = serde_json::from_value(event_json()).unwrap();
    event.entity_id = OTHER_OPERATION_ID.into();
    assert!(serde_json::to_value(&event).is_err());
}

#[test]
fn v2_control_event_sequence_is_epoch_scoped_and_independent_of_revision() {
    let rotated_epoch_event = event_json_at("123e4567-e89b-42d3-a456-426614174004", "1", "42");

    let event: V2ControlEvent = serde_json::from_value(rotated_epoch_event.clone()).unwrap();
    assert_eq!(serde_json::to_value(event).unwrap(), rotated_epoch_event);

    for (field, invalid) in [
        ("sequence", serde_json::json!("0")),
        ("revision", serde_json::json!("0")),
        ("sequence", serde_json::json!("01")),
        ("revision", serde_json::json!(42)),
    ] {
        let mut malformed = rotated_epoch_event.clone();
        malformed[field] = invalid;
        assert!(
            serde_json::from_value::<V2ControlEvent>(malformed).is_err(),
            "accepted invalid {field}"
        );
    }

    let mut missing_sequence = rotated_epoch_event.clone();
    missing_sequence.as_object_mut().unwrap().remove("sequence");
    assert!(serde_json::from_value::<V2ControlEvent>(missing_sequence).is_err());

    let mut sequence_above_revision = rotated_epoch_event.clone();
    sequence_above_revision["sequence"] = serde_json::json!("43");
    assert!(serde_json::from_value::<V2ControlEvent>(sequence_above_revision).is_err());

    let mut stale_operation = rotated_epoch_event;
    stale_operation["operation"]["updated_revision"] = serde_json::json!("41");
    assert!(serde_json::from_value::<V2ControlEvent>(stale_operation).is_err());
}

#[test]
fn v2_reconnect_cursor_is_epoch_scoped_and_independent_of_revision() {
    let event = event_json_at("123e4567-e89b-42d3-a456-426614174004", "1", "42");
    let snapshot = serde_json::json!({
        "schema_version": 2,
        "epoch": EPOCH,
        "revision": "42",
        "generated_at_unix_ms": "1784246400600",
        "stream": {"epoch": EPOCH, "cursor": "1", "cursor_gap": false},
        "nodes": [node_json(NODE_ID)],
        "slots": [slot_json(NODE_ID, "ready", None)],
        "operations": [],
        "events": [event]
    });

    let decoded: V2ReconnectSnapshot = serde_json::from_value(snapshot.clone()).unwrap();
    assert_eq!(serde_json::to_value(decoded).unwrap(), snapshot);

    for invalid_cursor in [serde_json::json!("0"), serde_json::json!("01")] {
        let mut invalid = snapshot.clone();
        invalid["stream"]["cursor"] = invalid_cursor;
        assert!(serde_json::from_value::<V2ReconnectSnapshot>(invalid).is_err());
    }
}

#[test]
fn v2_gap_free_reconnect_events_are_a_complete_tandem_sequence() {
    let first = event_json_at("123e4567-e89b-42d3-a456-426614174004", "1", "42");
    let second = event_json_at("123e4567-e89b-42d3-a456-426614174014", "2", "43");
    let snapshot = serde_json::json!({
        "schema_version": 2,
        "epoch": EPOCH,
        "revision": "43",
        "generated_at_unix_ms": "1784246400600",
        "stream": {"epoch": EPOCH, "cursor": "2", "cursor_gap": false},
        "nodes": [node_json(NODE_ID)],
        "slots": [slot_json(NODE_ID, "ready", None)],
        "operations": [],
        "events": [first, second]
    });
    assert!(serde_json::from_value::<V2ReconnectSnapshot>(snapshot.clone()).is_ok());

    let mut skipped_sequence = snapshot.clone();
    skipped_sequence["events"][1]["sequence"] = serde_json::json!("3");
    skipped_sequence["stream"]["cursor"] = serde_json::json!("3");
    assert!(serde_json::from_value::<V2ReconnectSnapshot>(skipped_sequence).is_err());

    let mut skipped_revision = snapshot.clone();
    skipped_revision["events"][1]["revision"] = serde_json::json!("44");
    skipped_revision["events"][1]["operation"]["updated_revision"] = serde_json::json!("44");
    skipped_revision["revision"] = serde_json::json!("44");
    assert!(serde_json::from_value::<V2ReconnectSnapshot>(skipped_revision).is_err());

    for revisions in [["42", "42"], ["42", "41"]] {
        let mut nonmonotonic = snapshot.clone();
        nonmonotonic["events"][1]["revision"] = serde_json::json!(revisions[1]);
        nonmonotonic["events"][1]["operation"]["updated_revision"] =
            serde_json::json!(revisions[1]);
        assert!(serde_json::from_value::<V2ReconnectSnapshot>(nonmonotonic).is_err());
    }

    let mut final_event_below_snapshot = snapshot;
    final_event_below_snapshot["stream"]["cursor"] = serde_json::json!("3");
    final_event_below_snapshot["revision"] = serde_json::json!("44");
    assert!(serde_json::from_value::<V2ReconnectSnapshot>(final_event_below_snapshot).is_err());

    let gap_snapshot = serde_json::json!({
        "schema_version": 2,
        "epoch": EPOCH,
        "revision": "43",
        "generated_at_unix_ms": "1784246400600",
        "stream": {"epoch": EPOCH, "cursor": "2", "cursor_gap": true},
        "nodes": [node_json(NODE_ID)],
        "slots": [slot_json(NODE_ID, "ready", None)],
        "operations": [],
        "events": []
    });
    assert!(serde_json::from_value::<V2ReconnectSnapshot>(gap_snapshot.clone()).is_ok());

    let mut gap_with_event = gap_snapshot;
    gap_with_event["events"] = serde_json::json!([event_json_at(
        "123e4567-e89b-42d3-a456-426614174014",
        "2",
        "43",
    )]);
    assert!(serde_json::from_value::<V2ReconnectSnapshot>(gap_with_event).is_err());
}

#[test]
fn v2_slot_wire_shape_remains_the_exact_seven_key_contract() {
    let exact = slot_json(NODE_ID, "ready", None);
    let slot: V2Slot = serde_json::from_value(exact.clone()).unwrap();
    assert_eq!(serde_json::to_value(slot).unwrap(), exact);
    assert_eq!(exact.as_object().unwrap().len(), 7);
    assert!(!exact.as_object().unwrap().contains_key("updated_revision"));
}
