//! Declared splits must pin the input watermark before their first batch.
//!
//! A source declares every split up front, but a fast split can produce its
//! first batch long before a slow one. The input watermark — the minimum across
//! splits — must include the not-yet-started declared splits so a window cannot
//! close, and drop the slow split's first records, on the fast split alone.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
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

fn ints(z: &ZSetBatch, column: usize) -> Vec<i64> {
    let array = z
        .batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (0..z.len()).map(|index| array.value(index)).collect()
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
fn declared_splits_pin_the_minimum_before_their_first_batch() {
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
    core.build_view(ViewId(0), &window_plan()).unwrap();

    // Both buckets are declared up front, but only the fast split 0 has
    // produced a batch yet.
    core.declare_splits(InputId(0), &[0, 1]).unwrap();
    core.push_split(InputId(0), 0, &inserts(&[(1, 100)]))
        .unwrap();
    // Split 1 is declared but silent, so the input minimum stays at its initial
    // value and window [0, 10) must not close on the fast split's watermark.
    assert!(core.snapshot(ViewId(0)).unwrap().is_empty());

    // Split 1's first record lands in [0, 10). Without the declaration, the
    // fast split's watermark (100) would have dropped it as late.
    core.push_split(InputId(0), 1, &inserts(&[(2, 5)])).unwrap();
    core.push_split(InputId(0), 1, &inserts(&[(2, 15)]))
        .unwrap();
    let snapshot = core.snapshot(ViewId(0)).unwrap();
    assert_eq!(ints(&snapshot, 1), vec![0], "window [0, 10) did not close");
    assert_eq!(
        ints(&snapshot, 2),
        vec![1],
        "window lost the slow split's first record"
    );
}
