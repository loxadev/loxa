use loxa_protocol::v1::{
    ControlErrorBody, ControlErrorCode, NodeIdentityProofResponse, NodeStatus,
    CONTROL_PROTOCOL_VERSION,
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
