//! Kernel restore guards on the snapshot's own identity fields.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use hotlap_core::snapshot::{InputSnapshot, ViewSnapshot};
use hotlap_core::{
    AggSpec, ENGINE_SNAPSHOT_FORMAT_VERSION, EngineSnapshot, IncrementalCore, InputId, Plan,
    ViewId, ZSetBatch,
};
use hotlap_engine::EngineCore;

fn input(id: u32) -> InputSnapshot {
    InputSnapshot {
        id: InputId(id),
        schema: None,
        spec: None,
        watermark: 0,
        splits: Vec::new(),
        late: 0,
    }
}

/// A snapshot naming the same view handle twice with different plans.
fn duplicate_snapshot() -> EngineSnapshot {
    let view = |plan: Plan| ViewSnapshot {
        id: ViewId(0),
        plan,
        windowed: false,
        tapped: false,
        output: None,
        pending: None,
        operators: vec![None],
    };
    EngineSnapshot {
        format_version: ENGINE_SNAPSHOT_FORMAT_VERSION,
        epoch: 0,
        frozen: false,
        inputs: vec![input(0), input(1)],
        views: vec![
            view(Plan::Source(InputId(0))),
            view(Plan::Source(InputId(1))),
        ],
    }
}

/// A populated engine with one built view and applied rows.
fn populated() -> EngineCore {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    let plan = Plan::GroupAggregate {
        input: Box::new(Plan::Source(InputId(0))),
        key: vec![0],
        aggs: vec![AggSpec::count()],
    };
    core.build_view(ViewId(0), &plan).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    let column: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2]));
    let batch = RecordBatch::try_new(schema, vec![column]).unwrap();
    let zset = ZSetBatch::new(batch, Arc::new(Int64Array::from(vec![1_i64, 1]))).unwrap();
    core.push(InputId(0), &zset).unwrap();
    core
}

/// A duplicate-handle snapshot must be rejected before any insert, leaving a
/// populated engine's views, plans and applied state untouched.
#[test]
fn restore_rejects_duplicate_view_ids_without_mutating() {
    let snapshot = duplicate_snapshot();
    let mut core = populated();
    let before = core.checkpoint().unwrap();
    assert!(
        !before.inputs.is_empty() && !before.views.is_empty(),
        "the engine must start populated"
    );

    let result = core.restore(&snapshot);
    assert!(
        result.is_err(),
        "duplicate view handles must be rejected before any mutation: {result:?}"
    );
    assert_eq!(
        core.checkpoint().unwrap(),
        before,
        "a rejected restore must not change the engine state or plans"
    );
}
