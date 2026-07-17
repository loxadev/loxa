use loxa_protocol::v1::{
    ControlErrorBody, ControlErrorCode, NodeIdentityProofResponse, NodeStatus,
    CONTROL_PROTOCOL_VERSION,
};
use loxa_protocol::v2::{
    DecimalU64, OperationId, StreamEpoch, V2ControlEvent, V2OperationAccepted, V2Slot,
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
