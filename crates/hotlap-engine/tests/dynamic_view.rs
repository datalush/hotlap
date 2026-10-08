//! Differential tests for views created after `START` from retained inputs.
//!
//! A view built after N pushes by replaying retained inputs must snapshot
//! exactly like the same view built before the first push, and must keep
//! updating with later pushes. Insufficient retention must be rejected.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use hotlap_core::{AggSpec, IncrementalCore, InputId, Plan, ViewId, WatermarkSpec, ZSetBatch};
use hotlap_engine::EngineCore;

fn schema(names: &[&str]) -> SchemaRef {
    Arc::new(Schema::new(
        names
            .iter()
            .map(|name| Field::new(*name, DataType::Int64, false))
            .collect::<Vec<_>>(),
    ))
}

/// A two-column `(k, ts)` Z-set from `(k, ts, diff)` rows.
fn events(rows: &[(i64, i64, i64)]) -> ZSetBatch {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(
            rows.iter().map(|row| row.0).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            rows.iter().map(|row| row.1).collect::<Vec<_>>(),
        )),
    ];
    let batch = RecordBatch::try_new(schema(&["k", "ts"]), columns).unwrap();
    ZSetBatch::new(
        batch,
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.2).collect::<Vec<_>>(),
        )),
    )
    .unwrap()
}

/// Materializes a Z-set as `(row values, diff)` pairs for comparison.
fn rows(zset: &ZSetBatch) -> Vec<(Vec<i64>, i64)> {
    let mut out = Vec::new();
    for index in 0..zset.len() {
        let values = zset
            .batch
            .columns()
            .iter()
            .map(|column| {
                column
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(index)
            })
            .collect();
        let diff = zset
            .diff()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(index);
        out.push((values, diff));
    }
    out
}

fn core(retention: Option<usize>) -> EngineCore {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.declare_watermark(
        InputId(0),
        WatermarkSpec {
            time_col: 1,
            lag: 0,
        },
    )
    .unwrap();
    if let Some(events) = retention {
        core.set_input_retention(events).unwrap();
    }
    core
}

fn group() -> Plan {
    Plan::GroupAggregate {
        input: Box::new(Plan::Source(InputId(0))),
        key: vec![0],
        aggs: vec![AggSpec::count()],
    }
}

fn window() -> Plan {
    Plan::TumbleCount {
        input: Box::new(Plan::Source(InputId(0))),
        key: vec![0],
        time_col: 1,
        size: 10,
    }
}

fn phase1(core: &mut EngineCore) {
    core.push(InputId(0), &events(&[(1, 0, 1), (2, 1, 1)]))
        .unwrap();
    core.push(InputId(0), &events(&[(1, 0, -1)])).unwrap();
    core.push(InputId(0), &events(&[(1, 2, 1), (3, 5, 1)]))
        .unwrap();
    // Close the windows starting at 0 and 10.
    core.push(InputId(0), &events(&[(9, 20, 1)])).unwrap();
}

fn phase2(core: &mut EngineCore) {
    core.push(InputId(0), &events(&[(2, 1, -1), (3, 3, 1)]))
        .unwrap();
    core.push(InputId(0), &events(&[(4, 25, 1)])).unwrap();
}

fn snapshots(a: &mut EngineCore, b: &mut EngineCore) {
    for view in 0..2 {
        assert_eq!(
            rows(&a.snapshot(ViewId(view)).unwrap()),
            rows(&b.snapshot(ViewId(view)).unwrap()),
            "late view {view} diverged from full recomputation"
        );
    }
}

#[test]
fn late_views_match_full_recomputation_and_keep_updating() {
    // Reference: group and window views built before the first push.
    let mut reference = core(None);
    reference.build_view(ViewId(0), &group()).unwrap();
    reference.build_view(ViewId(1), &window()).unwrap();

    // Dynamic: the same views built after the first phase, from retention.
    let mut dynamic = core(Some(64));
    phase1(&mut reference);
    phase1(&mut dynamic);
    dynamic.build_view(ViewId(0), &group()).unwrap();
    dynamic.build_view(ViewId(1), &window()).unwrap();
    snapshots(&mut reference, &mut dynamic);

    phase2(&mut reference);
    phase2(&mut dynamic);
    snapshots(&mut reference, &mut dynamic);
}

#[test]
fn late_view_without_retention_is_rejected() {
    let mut core = core(None);
    phase1(&mut core);
    assert!(core.build_view(ViewId(0), &group()).is_err());
}

#[test]
fn late_view_with_truncated_retention_is_rejected() {
    // Keep only two deltas, then push four: the oldest history is gone.
    let mut core = core(Some(2));
    phase1(&mut core);
    assert!(core.build_view(ViewId(0), &group()).is_err());
}
