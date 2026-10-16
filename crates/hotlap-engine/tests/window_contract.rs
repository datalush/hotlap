//! Current TumbleCount timestamp, boundary, and recovery contract.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use hotlap_core::{IncrementalCore, InputId, Plan, ViewId, WatermarkSpec, ZSetBatch};
use hotlap_engine::{EngineCore, ops::TumbleCount};

fn int_schema(nullable_time: bool) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("ts", DataType::Int64, nullable_time),
    ]))
}

fn ints(times: Vec<Option<i64>>, diffs: Vec<i64>) -> ZSetBatch {
    let keys: ArrayRef = Arc::new(Int64Array::from(vec![Some(1); times.len()]));
    let times_array: ArrayRef = Arc::new(Int64Array::from(times));
    make_batch(int_schema(true), keys, times_array, diffs)
}

fn make_batch(schema: Arc<Schema>, keys: ArrayRef, times: ArrayRef, diffs: Vec<i64>) -> ZSetBatch {
    let batch = RecordBatch::try_new(schema, vec![keys, times]).unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs))).unwrap()
}

fn output_rows(z: &ZSetBatch) -> Vec<(i64, i64, i64, i64)> {
    let col = |i| {
        z.batch
            .column(i)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
    };
    (0..z.len())
        .map(|i| {
            (
                col(0).value(i),
                col(1).value(i),
                col(2).value(i),
                z.diff
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(i),
            )
        })
        .collect()
}

#[test]
fn null_negative_and_min_timestamp_clamp_to_zero_and_boundaries_are_half_open() {
    let mut window = TumbleCount::new(&[0], 1, 10);
    let out = window
        .apply(
            &ints(
                vec![None, Some(-1), Some(i64::MIN), Some(9), Some(10), Some(11)],
                vec![1; 6],
            ),
            20,
        )
        .unwrap();
    assert_eq!(output_rows(&out), vec![(1, 0, 4, 1), (1, 10, 2, 1)]);
    assert_eq!(window.open_windows(), 0);
}

#[test]
fn arrow_timestamp_values_are_cast_as_raw_int64_ticks() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
    ]));
    let batch = make_batch(
        schema,
        Arc::new(Int64Array::from(vec![1])),
        Arc::new(TimestampNanosecondArray::from(vec![100_000_000])),
        vec![1],
    );
    let mut window = TumbleCount::new(&[0], 1, 10);
    let out = window.apply(&batch, i64::MAX).unwrap();
    assert_eq!(output_rows(&out), vec![(1, 100_000_000, 1, 1)]);
}

#[test]
fn nonpositive_width_is_rejected_and_maximum_width_closes_only_representable_end() {
    let batch = ints(vec![Some(0), Some(i64::MAX)], vec![1, 1]);
    for size in [0, -1] {
        assert!(
            TumbleCount::new(&[0], 1, size)
                .apply(&batch, i64::MAX)
                .is_err()
        );
    }
    let mut max_width = TumbleCount::new(&[0], 1, i64::MAX);
    let out = max_width.apply(&batch, i64::MAX).unwrap();
    assert_eq!(output_rows(&out), vec![(1, 0, 1, 1)]);
    assert_eq!(max_width.open_windows(), 1);

    let mut overflowing_end = TumbleCount::new(&[0], 1, 10);
    assert!(
        overflowing_end
            .apply(&ints(vec![Some(i64::MAX)], vec![1]), i64::MAX)
            .unwrap()
            .is_empty()
    );
    assert_eq!(overflowing_end.open_windows(), 1);
}

#[test]
fn closing_releases_open_buckets_while_returning_append_only_rows() {
    let mut window = TumbleCount::new(&[0], 1, 10);
    let out = window.apply(&ints(vec![Some(1)], vec![1]), 10).unwrap();
    assert_eq!(output_rows(&out), vec![(1, 0, 1, 1)]);
    assert_eq!(window.open_windows(), 0);
}

fn engine() -> EngineCore {
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
    core.declare_splits(InputId(0), &[0, 1]).unwrap();
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

fn event(key: i64, ts: i64, diff: i64) -> ZSetBatch {
    let schema = int_schema(false);
    let keys: ArrayRef = Arc::new(Int64Array::from(vec![key]));
    let times: ArrayRef = Arc::new(Int64Array::from(vec![ts]));
    make_batch(schema, keys, times, vec![diff])
}

#[test]
fn minimum_split_watermark_controls_closure_and_checkpoint_continuation() {
    let mut live = engine();
    live.push_split(InputId(0), 0, &event(1, 1, 1)).unwrap();
    live.push_split(InputId(0), 0, &event(2, 10, 1)).unwrap();
    assert!(live.snapshot(ViewId(0)).unwrap().is_empty());
    live.push_split(InputId(0), 1, &event(2, 9, 1)).unwrap();
    assert!(live.snapshot(ViewId(0)).unwrap().is_empty());

    let checkpoint = live.checkpoint().unwrap();
    let mut restored = EngineCore::new();
    restored.restore(&checkpoint).unwrap();
    for core in [&mut live, &mut restored] {
        core.push_split(InputId(0), 1, &event(2, 10, 1)).unwrap();
        assert_eq!(
            output_rows(&core.snapshot(ViewId(0)).unwrap()),
            vec![(1, 0, 1, 1), (2, 0, 1, 1)]
        );
        core.push_split(InputId(0), 1, &event(1, 1, -1)).unwrap();
        assert_eq!(core.window_late_closed(ViewId(0)).unwrap(), 1);
    }
    assert_eq!(
        output_rows(&live.snapshot(ViewId(0)).unwrap()),
        output_rows(&restored.snapshot(ViewId(0)).unwrap())
    );
    assert_eq!(
        output_rows(&live.take_changes(ViewId(0)).unwrap()),
        output_rows(&restored.take_changes(ViewId(0)).unwrap())
    );
}
