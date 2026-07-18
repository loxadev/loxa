use crate::control_state::{ControlIdGenerator, ControlRepository};
use loxa_protocol::v2::{EventId, SlotId, StreamEpoch};
use loxa_protocol::NodeId;
use std::fs;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};

const NODE_ID: &str = "91111111-1111-4111-8111-111111111111";
const SLOT_ID: &str = "92222222-2222-4222-8222-222222222222";
const EPOCH: &str = "93333333-3333-4333-8333-333333333333";
const EVENT_ID: &str = "95555555-5555-4555-8555-555555555555";
static NEXT_ROOT: AtomicU64 = AtomicU64::new(1);

struct TestRoot(PathBuf);

impl TestRoot {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "loxa-{label}-{}-{}",
            std::process::id(),
            NEXT_ROOT.fetch_add(1, Ordering::Relaxed)
        ));
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(&path).unwrap();
        Self(fs::canonicalize(path).unwrap())
    }

    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Ids;

impl ControlIdGenerator for Ids {
    fn new_slot_id(&mut self) -> SlotId {
        SlotId::from_str(SLOT_ID).unwrap()
    }

    fn new_stream_epoch(&mut self) -> StreamEpoch {
        StreamEpoch::from_str(EPOCH).unwrap()
    }

    fn new_initial_event_id(&mut self) -> EventId {
        EventId::from_str(EVENT_ID).unwrap()
    }
}

#[test]
fn fresh_persistence_has_one_desired_row_without_a_second_observed_projection() {
    let root = TestRoot::new("slice4-persistence-authority");
    let path = root.path().join("control-state.sqlite3");
    let repository =
        ControlRepository::open_or_create(&path, NodeId::from_str(NODE_ID).unwrap(), &mut Ids)
            .unwrap();

    repository
        .read_transaction(|connection| {
            let intent: (String, Option<String>, String, Option<String>, String) = connection
                .query_row(
                    "SELECT desired_kind,desired_model_id,desired_revision,operation_id,reconciliation_state FROM slot_intent WHERE singleton=1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
                )?;
            assert_eq!(
                intent,
                (
                    "unloaded".to_owned(),
                    None,
                    "1".to_owned(),
                    None,
                    "settled".to_owned(),
                )
            );
            let intent_columns = connection
                .prepare("PRAGMA table_info(slot_intent)")?
                .query_map([], |row| row.get::<_, String>(1))?
                .collect::<Result<Vec<_>, _>>()?;
            assert!(!intent_columns.iter().any(|column| {
                matches!(column.as_str(), "status" | "model_id" | "updated_revision")
            }));
            Ok(())
        })
        .unwrap();
    repository.close().unwrap();
}
