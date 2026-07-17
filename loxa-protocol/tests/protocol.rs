use loxa_protocol::v1::{
    ControlErrorBody, ControlErrorCode, NodeIdentityProofResponse, NodeStatus,
    CONTROL_PROTOCOL_VERSION,
};
use loxa_protocol::v2::{
    DecimalU64, OperationId, StreamEpoch, V2ControlErrorBody, V2ControlEvent, V2NodeCollection,
    V2OperationAccepted, V2OperationCollection, V2OperationEnvelope, V2OperationProgress,
    V2ReconnectSnapshot, V2Slot, V2SlotCollection,
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
        "ready" => (
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
