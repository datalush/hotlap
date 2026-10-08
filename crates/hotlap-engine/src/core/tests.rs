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

/// Number of raw (unconsolidated) rows retained by a view's output state.
fn output_rows(core: &EngineCore, view: ViewId) -> usize {
    core.views[&view].output.as_ref().map_or(0, |z| z.len())
}

#[test]
fn view_output_state_does_not_grow_with_history() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.build_view(ViewId(0), &group_plan()).unwrap();

    // Each push nets to zero, so the view's current state stays empty while a
    // history accumulator would retain rows proportional to the push count.
    let epochs = 200;
    for _ in 0..epochs {
        core.push(InputId(0), &zset(&[(1, 1), (2, 1), (1, -1), (2, -1)]))
            .unwrap();
    }

    assert!(
        output_rows(&core, ViewId(0)) <= 8,
        "view output state grew with history"
    );
    assert!(core.snapshot(ViewId(0)).unwrap().is_empty());
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
