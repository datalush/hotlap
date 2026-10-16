//! Differential checkpoint/restore tests.
//!
//! Continuing after `restore` must match continuing without a restart, across
//! several epochs, retractions and a tumbling window.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use hotlap_core::{
    AggSpec, EngineSnapshot, IncrementalCore, InputId, Plan, ViewId, WatermarkSpec, ZSetBatch,
};
use hotlap_engine::{EngineCore, decode_snapshot, encode_snapshot};

fn schema(names: &[&str]) -> SchemaRef {
    Arc::new(Schema::new(
        names
            .iter()
            .map(|name| Field::new(*name, DataType::Int64, false))
            .collect::<Vec<_>>(),
    ))
}

fn batch(schema: SchemaRef, columns: Vec<Vec<i64>>, diffs: Vec<i64>) -> ZSetBatch {
    let arrays: Vec<ArrayRef> = columns
        .into_iter()
        .map(|column| Arc::new(Int64Array::from(column)) as ArrayRef)
        .collect();
    let batch = RecordBatch::try_new(schema, arrays).unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs))).unwrap()
}

fn input0(rows: &[(i64, i64, i64, i64)]) -> ZSetBatch {
    batch(
        schema(&["k", "v", "ts"]),
        vec![
            rows.iter().map(|row| row.0).collect(),
            rows.iter().map(|row| row.1).collect(),
            rows.iter().map(|row| row.2).collect(),
        ],
        rows.iter().map(|row| row.3).collect(),
    )
}

fn input1(rows: &[(i64, i64, i64)]) -> ZSetBatch {
    batch(
        schema(&["k", "w"]),
        vec![
            rows.iter().map(|row| row.0).collect(),
            rows.iter().map(|row| row.1).collect(),
        ],
        rows.iter().map(|row| row.2).collect(),
    )
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
            .collect::<Vec<i64>>();
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

/// Registers two sources (both with watermarks) and builds group, window and
/// join views, tapping all of them.
fn engine() -> EngineCore {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.register_input(InputId(1)).unwrap();
    core.declare_watermark(
        InputId(0),
        WatermarkSpec {
            time_col: 2,
            lag: 0,
        },
    )
    .unwrap();
    core.declare_watermark(
        InputId(1),
        WatermarkSpec {
            time_col: 1,
            lag: 0,
        },
    )
    .unwrap();

    let group = Plan::GroupAggregate {
        input: Box::new(Plan::Source(InputId(0))),
        key: vec![0],
        aggs: vec![AggSpec::count()],
    };
    let window = Plan::TumbleCount {
        input: Box::new(Plan::Source(InputId(0))),
        key: vec![0],
        time_col: 2,
        size: 10,
    };
    let join = Plan::Join {
        left: Box::new(Plan::Source(InputId(0))),
        right: Box::new(Plan::Source(InputId(1))),
        left_key: vec![0],
        right_key: vec![0],
    };
    core.build_view(ViewId(0), &group).unwrap();
    core.build_view(ViewId(1), &window).unwrap();
    core.build_view(ViewId(2), &join).unwrap();
    for view in 0..3 {
        core.tap_view(ViewId(view)).unwrap();
    }
    core
}

fn phase1(core: &mut EngineCore) {
    core.push(InputId(0), &input0(&[(1, 10, 0, 1), (2, 20, 1, 1)]))
        .unwrap();
    core.push(InputId(0), &input0(&[(1, 10, 0, -1)])).unwrap();
    core.push(InputId(0), &input0(&[(1, 11, 2, 1), (3, 30, 5, 1)]))
        .unwrap();
    // Advance the watermark far enough to close windows starting at 0 and 10.
    core.push(InputId(0), &input0(&[(9, 90, 20, 1)])).unwrap();
    core.push(InputId(1), &input1(&[(1, 100, 1), (2, 200, 1)]))
        .unwrap();
    core.push(InputId(1), &input1(&[(1, 100, -1)])).unwrap();
}

fn phase2(core: &mut EngineCore) {
    core.push(InputId(0), &input0(&[(2, 20, 1, -1), (3, 31, 3, 1)]))
        .unwrap();
    core.push(InputId(0), &input0(&[(4, 40, 25, 1)])).unwrap();
    core.push(InputId(1), &input1(&[(3, 300, 1), (2, 200, -1)]))
        .unwrap();
}

#[test]
fn restore_then_continue_matches_no_restart() {
    let mut original = engine();
    phase1(&mut original);
    let snapshot = original.checkpoint().unwrap();

    // Round-trip through the binary codec must be stable and restore idempotent.
    let bytes = encode_snapshot(&snapshot).unwrap();
    let decoded: EngineSnapshot = decode_snapshot(&bytes).unwrap();
    assert_eq!(snapshot, decoded, "snapshot bytes are not stable");

    let mut restored = EngineCore::new();
    restored.restore(&decoded).unwrap();
    assert_eq!(
        original.checkpoint().unwrap(),
        restored.checkpoint().unwrap(),
        "restored engine does not re-checkpoint identically"
    );

    phase2(&mut original);
    phase2(&mut restored);

    for view in 0..3 {
        assert_eq!(
            rows(&original.snapshot(ViewId(view)).unwrap()),
            rows(&restored.snapshot(ViewId(view)).unwrap()),
            "view {view} snapshot diverged after restore"
        );
        assert_eq!(
            rows(&original.take_changes(ViewId(view)).unwrap()),
            rows(&restored.take_changes(ViewId(view)).unwrap()),
            "view {view} changelog diverged after restore"
        );
    }
    for input in 0..2 {
        assert_eq!(
            original.late_dropped(InputId(input)).unwrap(),
            restored.late_dropped(InputId(input)).unwrap(),
            "input {input} late counter diverged after restore"
        );
    }
}

#[test]
fn window_checkpoint_without_schema_but_with_open_bucket_is_rejected() {
    let mut original = EngineCore::new();
    original.register_input(InputId(0)).unwrap();
    original
        .declare_watermark(
            InputId(0),
            WatermarkSpec {
                time_col: 2,
                lag: 0,
            },
        )
        .unwrap();
    original
        .build_view(
            ViewId(0),
            &Plan::TumbleCount {
                input: Box::new(Plan::Source(InputId(0))),
                key: vec![0],
                time_col: 2,
                size: 10,
            },
        )
        .unwrap();
    original.push(InputId(0), &input0(&[(1, 2, 1, 1)])).unwrap();
    let mut corrupted = original.checkpoint().unwrap();
    let window = match corrupted.views[0].operators[0].as_mut().unwrap() {
        hotlap_core::snapshot::OperatorState::Window(state) => state,
        other => panic!("expected window state, got {other:?}"),
    };
    window.schema = None;
    let bytes = encode_snapshot(&corrupted).unwrap();
    let decoded = decode_snapshot(&bytes).unwrap();
    assert_rejected_restore_preserves_window_engine(decoded);
}

#[test]
fn window_checkpoint_duplicate_open_bucket_is_rejected() {
    let mut original = EngineCore::new();
    original.register_input(InputId(0)).unwrap();
    original
        .declare_watermark(
            InputId(0),
            WatermarkSpec {
                time_col: 2,
                lag: 0,
            },
        )
        .unwrap();
    original
        .build_view(
            ViewId(0),
            &Plan::TumbleCount {
                input: Box::new(Plan::Source(InputId(0))),
                key: vec![0],
                time_col: 2,
                size: 10,
            },
        )
        .unwrap();
    original.push(InputId(0), &input0(&[(1, 2, 1, 1)])).unwrap();
    let mut corrupted = original.checkpoint().unwrap();
    let state = match corrupted.views[0].operators[0].as_mut().unwrap() {
        hotlap_core::snapshot::OperatorState::Window(state) => state,
        other => panic!("expected window state, got {other:?}"),
    };
    state.open.push(state.open[0].clone());
    let decoded = decode_snapshot(&encode_snapshot(&corrupted).unwrap()).unwrap();
    assert_rejected_restore_preserves_window_engine(decoded);
}

#[test]
fn empty_uninitialized_window_checkpoint_restores() {
    let original = window_engine();
    let snapshot = original.checkpoint().unwrap();
    let mut restored = EngineCore::new();
    restored.restore(&snapshot).unwrap();
    assert_eq!(restored.checkpoint().unwrap(), snapshot);
}

#[test]
fn restore_preserves_negative_and_zero_open_window_counts() {
    let mut original = window_engine();
    original
        .push(InputId(0), &input0(&[(1, 0, 1, -2), (2, 0, 2, 0)]))
        .unwrap();
    let checkpoint = original.checkpoint().unwrap();
    let state = match checkpoint.views[0].operators[0].as_ref().unwrap() {
        hotlap_core::snapshot::OperatorState::Window(state) => state,
        other => panic!("expected window state, got {other:?}"),
    };
    let mut counts = state
        .open
        .iter()
        .map(|bucket| bucket.count)
        .collect::<Vec<_>>();
    counts.sort();
    assert_eq!(counts, vec![-2, 0]);

    let mut restored = EngineCore::new();
    restored.restore(&checkpoint).unwrap();
    restored
        .push(InputId(0), &input0(&[(3, 0, 10, 1)]))
        .unwrap();
    assert_eq!(
        rows(&restored.snapshot(ViewId(0)).unwrap()),
        vec![(vec![1, 0, -2], 1)]
    );
}

#[test]
#[ignore = "known current-v1 recovery defect; tracked by hotlap#qs79; this test must fail when run"]
fn window_checkpoint_truncated_key_parser_behavior_probe() {
    let mut original = window_engine();
    original.push(InputId(0), &input0(&[(1, 2, 1, 1)])).unwrap();
    let mut corrupted = original.checkpoint().unwrap();
    let state = match corrupted.views[0].operators[0].as_mut().unwrap() {
        hotlap_core::snapshot::OperatorState::Window(state) => state,
        other => panic!("expected window state, got {other:?}"),
    };
    state.open[0].key.truncate(1);
    let decoded = decode_snapshot(&encode_snapshot(&corrupted).unwrap()).unwrap();
    let mut target = EngineCore::new();
    let result =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| target.restore(&decoded)));
    assert!(
        matches!(result, Ok(Err(_))),
        "malformed key was not rejected with a typed error: {result:?}"
    );
}

fn window_engine() -> EngineCore {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.declare_watermark(
        InputId(0),
        WatermarkSpec {
            time_col: 2,
            lag: 0,
        },
    )
    .unwrap();
    core.build_view(
        ViewId(0),
        &Plan::TumbleCount {
            input: Box::new(Plan::Source(InputId(0))),
            key: vec![0],
            time_col: 2,
            size: 10,
        },
    )
    .unwrap();
    core.tap_view(ViewId(0)).unwrap();
    core
}

fn assert_rejected_restore_preserves_window_engine(snapshot: EngineSnapshot) {
    let mut target = window_engine();
    target.push(InputId(0), &input0(&[(7, 0, 1, 1)])).unwrap();
    target.push(InputId(0), &input0(&[(7, 0, 10, 1)])).unwrap();
    assert_eq!(
        rows(&target.snapshot(ViewId(0)).unwrap()),
        vec![(vec![7, 0, 1], 1)]
    );
    target.take_changes(ViewId(0)).unwrap();

    let before = target.checkpoint().unwrap();
    let mut reference = EngineCore::new();
    reference.restore(&before).unwrap();
    assert!(
        matches!(
            target.restore(&snapshot),
            Err(hotlap_engine::EngineError::Infrastructure(_))
        ),
        "corrupt window state did not return typed Infrastructure error"
    );
    assert_eq!(
        target.checkpoint().unwrap(),
        before,
        "failed restore mutated target"
    );

    for batch in [input0(&[(8, 0, 12, 1)]), input0(&[(8, 0, 20, 1)])] {
        target.push(InputId(0), &batch).unwrap();
        reference.push(InputId(0), &batch).unwrap();
    }
    assert_eq!(
        rows(&target.snapshot(ViewId(0)).unwrap()),
        vec![(vec![7, 0, 1], 1), (vec![7, 10, 1], 1), (vec![8, 10, 1], 1),]
    );
    assert_eq!(
        rows(&target.snapshot(ViewId(0)).unwrap()),
        rows(&reference.snapshot(ViewId(0)).unwrap())
    );
    assert_eq!(
        rows(&target.take_changes(ViewId(0)).unwrap()),
        rows(&reference.take_changes(ViewId(0)).unwrap())
    );
}
