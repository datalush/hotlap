//! Differential tests for per-split watermarks (minimum across splits).
//!
//! A fast split must not advance the input watermark past a slower one, so
//! valid records of the slow split are neither late-dropped nor used to close
//! windows prematurely.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use hotlap_core::{IncrementalCore, InputId, Plan, ViewId, WatermarkSpec, ZSetBatch};
use hotlap_engine::EngineCore;

fn time_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
    ]))
}

/// Builds a Z-set of `(key, event_time)` insertions over the time schema.
fn inserts(rows: &[(i64, i64)]) -> ZSetBatch {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(
            rows.iter().map(|row| row.0).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            rows.iter().map(|row| row.1).collect::<Vec<_>>(),
        )),
    ];
    let diffs = vec![1i64; rows.len()];
    let batch = RecordBatch::try_new(time_schema(), columns).unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs))).unwrap()
}

fn keys(z: &ZSetBatch) -> Vec<i64> {
    let column = z
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (0..z.len()).map(|index| column.value(index)).collect()
}

fn ints(z: &ZSetBatch, column: usize) -> Vec<i64> {
    let array = z
        .batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (0..z.len()).map(|index| array.value(index)).collect()
}

fn engine_with(plan: Plan) -> EngineCore {
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
    core.build_view(ViewId(0), &plan).unwrap();
    core
}

fn group_plan() -> Plan {
    Plan::GroupCount {
        input: Box::new(Plan::Source(InputId(0))),
        key: vec![0],
    }
}

#[test]
fn slow_split_records_survive_a_fast_split_watermark() {
    let mut core = engine_with(group_plan());
    // Split 1 is slow (low timestamps); split 0 races ahead.
    core.push_split(InputId(0), 1, &inserts(&[(1, 5)])).unwrap();
    core.push_split(InputId(0), 0, &inserts(&[(2, 100)]))
        .unwrap();
    // ts 8 < the fast split's 100, but >= the slow split's own watermark 5:
    // it must not be late-dropped.
    core.push_split(InputId(0), 1, &inserts(&[(3, 8)])).unwrap();

    assert_eq!(core.late_dropped(InputId(0)).unwrap(), 0);
    let mut counts = keys(&core.snapshot(ViewId(0)).unwrap());
    counts.sort();
    assert_eq!(counts, vec![1, 2, 3]);
}

fn window_plan() -> Plan {
    Plan::TumbleCount {
        input: Box::new(Plan::Source(InputId(0))),
        key: vec![0],
        time_col: 1,
        size: 10,
    }
}

#[test]
fn window_close_uses_the_minimum_across_splits() {
    let mut core = engine_with(window_plan());

    // Only the slow split feeds window [0, 10); the fast split is far ahead.
    core.push_split(InputId(0), 1, &inserts(&[(1, 5)])).unwrap();
    core.push_split(InputId(0), 0, &inserts(&[(2, 100)]))
        .unwrap();
    // Using the fast split's watermark (100) this would emit; using the min
    // (5) the window stays open.
    assert!(core.snapshot(ViewId(0)).unwrap().is_empty());

    // A second record in [0, 10) arrives before the slow split catches up.
    core.push_split(InputId(0), 1, &inserts(&[(1, 8)])).unwrap();
    assert!(core.snapshot(ViewId(0)).unwrap().is_empty());

    // The slow split reaches 15, so the minimum reaches the window end.
    core.push_split(InputId(0), 1, &inserts(&[(1, 15)])).unwrap();
    let snapshot = core.snapshot(ViewId(0)).unwrap();
    assert_eq!(ints(&snapshot, 1), vec![0], "window [0, 10) did not close");
    assert_eq!(ints(&snapshot, 2), vec![2], "window missed slow records");

    // Reference: the same records as one stream emit the same closed window.
    let mut reference = engine_with(window_plan());
    for rows in [[(1, 5)], [(1, 8)], [(1, 100)]] {
        reference.push(InputId(0), &inserts(&rows)).unwrap();
    }
    let reference = reference.snapshot(ViewId(0)).unwrap();
    assert_eq!(ints(&snapshot, 0), ints(&reference, 0));
    assert_eq!(ints(&snapshot, 1), ints(&reference, 1));
    assert_eq!(ints(&snapshot, 2), ints(&reference, 2));
}
