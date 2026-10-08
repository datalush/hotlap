//! Unit tests for the bounded view state.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use hotlap_core::{IncrementalCore, InputId, Plan, ViewId, ZSetBatch};

use super::EngineCore;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]))
}

/// Builds a single-column Z-set of `(key, diff)` rows.
fn zset(rows: &[(i64, i64)]) -> ZSetBatch {
    let keys: Vec<i64> = rows.iter().map(|row| row.0).collect();
    let diffs: Vec<i64> = rows.iter().map(|row| row.1).collect();
    let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(keys))];
    let batch = RecordBatch::try_new(schema(), columns).unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs))).unwrap()
}

fn group_plan() -> Plan {
    Plan::GroupCount {
        input: Box::new(Plan::Source(InputId(0))),
        key: vec![0],
    }
}

/// Number of consolidated rows retained by a view's output map.
fn output_rows(core: &EngineCore, view: ViewId) -> usize {
    core.views[&view].output.len()
}

#[test]
fn view_output_state_does_not_grow_with_history() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.build_view(ViewId(0), &Plan::Source(InputId(0))).unwrap();

    // Every push is a single non-zero row. Insert each key once, then retract
    // and re-insert as the pattern cycles: the output map never exceeds the
    // distinct-key count no matter how many pushes run.
    let keys = 10i64;
    let epochs = 200;
    for epoch in 0..epochs {
        let key = epoch % keys;
        let diff = if (epoch / keys) % 2 == 0 { 1 } else { -1 };
        core.push(InputId(0), &zset(&[(key, diff)])).unwrap();
    }

    assert!(
        output_rows(&core, ViewId(0)) <= keys as usize,
        "view output state grew with history"
    );
    // The final cycle was a retraction block, so the net state is empty.
    assert!(core.snapshot(ViewId(0)).unwrap().is_empty());
}

#[test]
fn row_converter_is_built_once_not_per_push() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.build_view(ViewId(0), &group_plan()).unwrap();
    core.push(InputId(0), &zset(&[(1, 1)])).unwrap();

    // After the first push freezes the schema, further pushes must reuse the
    // cached converters instead of building O(pushes) of them.
    crate::work::reset();
    for _ in 0..200 {
        core.push(InputId(0), &zset(&[(1, 1)])).unwrap();
    }
    assert_eq!(
        crate::work::converter_builds(),
        0,
        "row converters were rebuilt per push"
    );
}

#[test]
fn view_output_state_tracks_current_state_not_history() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.build_view(ViewId(0), &group_plan()).unwrap();

    // One logical row (`key 1`) updated many times: the current state is one
    // row regardless of how many deltas produced it.
    for _ in 0..100 {
        core.push(InputId(0), &zset(&[(1, 1)])).unwrap();
    }

    assert_eq!(output_rows(&core, ViewId(0)), 1);
    assert_eq!(core.snapshot(ViewId(0)).unwrap().len(), 1);
}

/// A one-row push must encode only its delta, not the resident state.
///
/// With a large resident state this fails as soon as push falls back to
/// consolidating the whole state (which re-encodes every retained row).
#[test]
fn push_encodes_only_the_delta() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.build_view(ViewId(0), &group_plan()).unwrap();

    // Seed a large resident state with distinct keys.
    let seed: Vec<(i64, i64)> = (0..2_000).map(|key| (key, 1)).collect();
    core.push(InputId(0), &zset(&seed)).unwrap();
    assert_eq!(output_rows(&core, ViewId(0)), 2_000);

    // A push of one new key must encode only its delta, not the resident state.
    crate::work::reset();
    core.push(InputId(0), &zset(&[(10_000, 1)])).unwrap();
    assert_eq!(output_rows(&core, ViewId(0)), 2_001);
    assert!(
        crate::work::encoded_rows() <= 2,
        "1-row push re-encoded the resident state"
    );
}
