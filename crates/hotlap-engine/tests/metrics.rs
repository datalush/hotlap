//! Metrics counters wired into the engine core.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use hotlap_core::{
    IncrementalCore, InputId, MetricsRegistry, Plan, ViewId, WatermarkSpec, ZSetBatch,
};
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

/// Builds a tumbling-window view over `(key, event_time)` with size 10.
fn window_core(metrics: Arc<MetricsRegistry>) -> EngineCore {
    let mut core = EngineCore::with_metrics(metrics);
    core.register_input(InputId(0)).unwrap();
    core.declare_watermark(
        InputId(0),
        WatermarkSpec {
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
    core
}

#[test]
fn counters_track_ingest_emit_late_and_checkpoints() {
    let metrics = Arc::new(MetricsRegistry::new());
    let mut core = window_core(Arc::clone(&metrics));

    // Two rows join window [0, 10); nothing is emitted while it stays open.
    core.push(InputId(0), &time_zset(&[(1, 1, 1), (1, 2, 1)]))
        .unwrap();
    // ts 12 closes [0, 10) and emits one window row.
    core.push(InputId(0), &time_zset(&[(1, 12, 1)])).unwrap();
    // A late insertion (ts 3 below the watermark) is dropped as late.
    core.push(InputId(0), &time_zset(&[(1, 3, 1)])).unwrap();
    // A retraction for the already-closed window is dropped as late-closed.
    core.push(InputId(0), &time_zset(&[(1, 3, -1)])).unwrap();

    let snapshot = core.checkpoint().unwrap();
    let mut restored = EngineCore::with_metrics(Arc::clone(&metrics));
    restored.restore(&snapshot).unwrap();

    let counts = metrics.snapshot();
    assert_eq!(counts.get("rows_ingested"), Some(&5));
    assert_eq!(counts.get("rows_emitted"), Some(&1));
    assert_eq!(counts.get("late_dropped"), Some(&1));
    assert_eq!(counts.get("late_closed_dropped"), Some(&1));
    assert_eq!(counts.get("checkpoints_taken"), Some(&1));
    assert_eq!(counts.get("checkpoints_restored"), Some(&1));
}

#[test]
fn windows_open_gauge_tracks_open_windows() {
    let metrics = Arc::new(MetricsRegistry::new());
    let mut core = window_core(Arc::clone(&metrics));

    // One event opens window [0, 10).
    core.push(InputId(0), &time_zset(&[(1, 1, 1)])).unwrap();
    assert_eq!(metrics.snapshot().get("windows_open"), Some(&1));
    // A second key in the same window start does not open another window.
    core.push(InputId(0), &time_zset(&[(2, 2, 1)])).unwrap();
    assert_eq!(metrics.snapshot().get("windows_open"), Some(&1));
    // ts 12 closes [0, 10) and opens [10, 20), so one window stays open.
    core.push(InputId(0), &time_zset(&[(1, 12, 1)])).unwrap();
    assert_eq!(metrics.snapshot().get("windows_open"), Some(&1));
}

#[test]
fn cores_without_injection_get_their_own_registry() {
    let mut core = window_core(Arc::new(MetricsRegistry::new()));
    core.push(InputId(0), &time_zset(&[(1, 1, 1)])).unwrap();

    assert_eq!(core.metrics().snapshot().get("rows_ingested"), Some(&1));
}
