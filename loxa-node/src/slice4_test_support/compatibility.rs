use loxa_protocol::v2::{DecimalU64, OperationId, V2Operation, V2OperationKind, V2OperationStatus};
use loxa_protocol::NodeId;

fn operation(
    operation_id: &str,
    kind: V2OperationKind,
    status: V2OperationStatus,
    created_revision: u64,
) -> V2Operation {
    let lifecycle = matches!(kind, V2OperationKind::Load | V2OperationKind::Unload);
    V2Operation {
        operation_id: operation_id.parse().unwrap(),
        node_id: "123e4567-e89b-42d3-a456-426614174000"
            .parse::<NodeId>()
            .unwrap(),
        kind,
        status,
        slot_id: lifecycle.then(|| "123e4567-e89b-42d3-8456-426614174002".parse().unwrap()),
        model_id: (kind != V2OperationKind::Unload).then(|| "gemma-3-4b-it-q4".into()),
        progress: None,
        error: None,
        created_revision: DecimalU64::new(created_revision),
        updated_revision: DecimalU64::new(created_revision),
        created_at_unix_ms: DecimalU64::new(created_revision),
        updated_at_unix_ms: DecimalU64::new(created_revision),
    }
}

fn active(operation: &V2Operation) -> bool {
    matches!(
        operation.status,
        V2OperationStatus::Queued | V2OperationStatus::Running | V2OperationStatus::Cancelling
    )
}

fn presentation_key(operation: &V2Operation) -> (u8, DecimalU64, OperationId) {
    let lane = match operation.kind {
        V2OperationKind::Load | V2OperationKind::Unload => 0,
        V2OperationKind::Download => 1,
    };
    let status = match operation.status {
        V2OperationStatus::Cancelling => 0,
        V2OperationStatus::Running => 1,
        V2OperationStatus::Queued => 2,
        _ => 3,
    };
    (
        lane * 4 + status,
        operation.created_revision,
        operation.operation_id,
    )
}

#[test]
fn presentation_prioritizes_active_lifecycle_and_keeps_canonical_collection_unchanged() {
    let canonical = vec![
        operation(
            "11111111-1111-4111-8111-111111111111",
            V2OperationKind::Load,
            V2OperationStatus::Running,
            1,
        ),
        operation(
            "22222222-2222-4222-8222-222222222222",
            V2OperationKind::Download,
            V2OperationStatus::Running,
            2,
        ),
        operation(
            "33333333-3333-4333-8333-333333333333",
            V2OperationKind::Download,
            V2OperationStatus::Cancelling,
            3,
        ),
        operation(
            "44444444-4444-4444-8444-444444444444",
            V2OperationKind::Download,
            V2OperationStatus::Queued,
            4,
        ),
        operation(
            "55555555-5555-4555-8555-555555555555",
            V2OperationKind::Download,
            V2OperationStatus::Queued,
            4,
        ),
        operation(
            "66666666-6666-4666-8666-666666666666",
            V2OperationKind::Download,
            V2OperationStatus::Queued,
            5,
        ),
    ];
    assert!(canonical
        .iter()
        .all(|operation| operation.validate().is_ok()));
    let canonical_ids = canonical
        .iter()
        .map(|operation| operation.operation_id)
        .collect::<Vec<_>>();
    let mut active_operations = canonical
        .iter()
        .filter(|operation| active(operation))
        .collect::<Vec<_>>();
    active_operations.sort_by_key(|operation| presentation_key(operation));

    assert_eq!(active_operations.len(), 6);
    assert_eq!(
        active_operations
            .iter()
            .take(5)
            .map(|operation| operation.operation_id)
            .collect::<Vec<_>>(),
        vec![
            "11111111-1111-4111-8111-111111111111"
                .parse::<OperationId>()
                .unwrap(),
            "33333333-3333-4333-8333-333333333333".parse().unwrap(),
            "22222222-2222-4222-8222-222222222222".parse().unwrap(),
            "44444444-4444-4444-8444-444444444444".parse().unwrap(),
            "55555555-5555-4555-8555-555555555555".parse().unwrap(),
        ]
    );
    assert_eq!(active_operations.len() - 5, 1);
    assert_eq!(
        canonical
            .iter()
            .map(|operation| operation.operation_id)
            .collect::<Vec<_>>(),
        canonical_ids
    );
}
