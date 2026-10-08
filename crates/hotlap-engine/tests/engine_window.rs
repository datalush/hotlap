//! Engine-level tumbling-window tests over event-time inputs.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use hotlap_core::{IncrementalCore, InputId, Plan, ViewId, ZSetBatch};
use hotlap_engine::EngineCore;

fn time_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
    ]))
}

/// Builds a Z-set of `(key, event_time, diff)` triples over the time schema.
fn time_zset(rows: &[(i64, i64, i64)]) -> ZSetBatch {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(
            rows.iter().map(|row| row.0).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            rows.iter().map(|row| row.1).collect::<Vec<_>>(),
        )),
    ];
    let diffs: Vec<i64> = rows.iter().map(|row| row.2).collect();
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

#[test]
fn tumbling_window_closes_and_drops_late_records() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.declare_watermark(
        InputId(0),
        hotlap_core::WatermarkSpec {
            time_col: 1,
            lag: 0,
        },
    )
    .unwrap();
    core.build_view(
        ViewId(0),
        &Plan::TumbleCount {
            input: Box::new(Plan::Source(InputId(0))),
            key: vec![0],
            time_col: 1,
            size: 10,
        },
    )
    .unwrap();
    core.tap_view(ViewId(0)).unwrap();

    // Two events in window [0, 10): not closed until the watermark reaches 10.
    core.push(InputId(0), &time_zset(&[(1, 1, 1), (1, 2, 1)]))
        .unwrap();
    assert!(core.snapshot(ViewId(0)).unwrap().is_empty());

    // Event at ts 12 advances the watermark and closes [0, 10).
    core.push(InputId(0), &time_zset(&[(1, 12, 1)])).unwrap();
    let snapshot = core.snapshot(ViewId(0)).unwrap();
    assert_eq!(ints(&snapshot, 0), vec![1]);
    assert_eq!(ints(&snapshot, 1), vec![0]);
    assert_eq!(ints(&snapshot, 2), vec![2]);

    // A late insertion (ts 3 < watermark 12) is dropped and counted.
    core.push(InputId(0), &time_zset(&[(1, 3, 1)])).unwrap();
    assert_eq!(core.late_dropped(InputId(0)).unwrap(), 1);
    assert_eq!(core.snapshot(ViewId(0)).unwrap().len(), 1);
}

#[test]
fn window_late_closed_metric_counts_closed_window_deltas() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.declare_watermark(
        InputId(0),
        hotlap_core::WatermarkSpec {
            time_col: 1,
            lag: 0,
        },
    )
    .unwrap();
    core.build_view(
        ViewId(0),
        &Plan::TumbleCount {
            input: Box::new(Plan::Source(InputId(0))),
            key: vec![0],
            time_col: 1,
            size: 10,
        },
    )
    .unwrap();
    core.tap_view(ViewId(0)).unwrap();

    // Close [0, 10) by advancing the watermark past it.
    core.push(InputId(0), &time_zset(&[(1, 1, 1), (1, 2, 1)]))
        .unwrap();
    core.push(InputId(0), &time_zset(&[(1, 12, 1)])).unwrap();
    assert_eq!(core.window_late_closed(ViewId(0)).unwrap(), 0);

    // A retraction for the already-emitted window is kept by filter_late (only
    // insertions are late-dropped) but dropped by the window as already closed.
    core.push(InputId(0), &time_zset(&[(1, 3, -1)])).unwrap();
    assert_eq!(core.window_late_closed(ViewId(0)).unwrap(), 1);
}

#[test]
fn below_watermark_retraction_is_applied_not_dropped() {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.declare_watermark(
        InputId(0),
        hotlap_core::WatermarkSpec {
            time_col: 1,
            lag: 0,
        },
    )
    .unwrap();
    core.build_view(
        ViewId(0),
        &Plan::TumbleCount {
            input: Box::new(Plan::Source(InputId(0))),
            key: vec![0],
            time_col: 1,
            size: 10,
        },
    )
    .unwrap();
    core.tap_view(ViewId(0)).unwrap();

    core.push(InputId(0), &time_zset(&[(1, 1, 1), (1, 5, 1)]))
        .unwrap();
    // Retraction below watermark 5: filter_late keeps it, and the window applies
    // it to the still-open [0, 10) bucket.
    core.push(InputId(0), &time_zset(&[(1, 1, -1)])).unwrap();
    // Closing [0, 10) emits the corrected count of 1, not 2.
    core.push(InputId(0), &time_zset(&[(1, 10, 1)])).unwrap();

    let snapshot = core.snapshot(ViewId(0)).unwrap();
    assert_eq!(ints(&snapshot, 0), vec![1]);
    assert_eq!(ints(&snapshot, 1), vec![0]);
    assert_eq!(ints(&snapshot, 2), vec![1]);
}
