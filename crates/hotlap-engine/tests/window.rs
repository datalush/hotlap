use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::compute::{concat, concat_batches};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use hotlap_engine::ZSetBatch;
use hotlap_engine::consolidate;
use hotlap_engine::ops::TumbleCount;
use hotlap_engine::time::Watermark;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
    ]))
}

/// Builds a Z-set of `(key, event_time, diff)` triples over the shared schema.
fn zset(rows: &[(i64, i64, i64)]) -> ZSetBatch {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.1).collect::<Vec<_>>(),
        )),
    ];
    let diffs: Vec<i64> = rows.iter().map(|r| r.2).collect();
    let batch = RecordBatch::try_new(schema(), columns).unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs))).unwrap()
}

/// Reads an emitted Z-set as sorted `(key, window_start, count, diff)` rows.
fn rows(z: &ZSetBatch) -> Vec<(i64, i64, i64, i64)> {
    let keys = ints(z, 0);
    let starts = ints(z, 1);
    let counts = ints(z, 2);
    let diffs = diff_ints(z);
    let mut out: Vec<(i64, i64, i64, i64)> = (0..z.len())
        .map(|i| (keys[i], starts[i], counts[i], diffs[i]))
        .collect();
    out.sort();
    out
}

fn diff_ints(z: &ZSetBatch) -> Vec<i64> {
    let array = z.diff.as_any().downcast_ref::<Int64Array>().unwrap();
    (0..z.len()).map(|index| array.value(index)).collect()
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

/// Concatenates the changelogs and consolidates them to net emitted rows.
fn consolidated(changelogs: &[ZSetBatch]) -> Vec<(i64, i64, i64, i64)> {
    let schema = changelogs[0].schema();
    let batches: Vec<&RecordBatch> = changelogs.iter().map(|z| &z.batch).collect();
    let batch = concat_batches(&schema, batches).unwrap();
    let diffs: Vec<&dyn Array> = changelogs.iter().map(|z| z.diff.as_ref()).collect();
    let all = ZSetBatch::new(batch, concat(&diffs).unwrap()).unwrap();
    rows(&consolidate(&all).unwrap())
}

/// Full-recomputation oracle: per-batch late-drop against the previous
/// watermark, then keep the non-empty windows closed by the final watermark.
fn recompute(batches: &[Vec<(i64, i64, i64)>], size: i64, lag: i64) -> Vec<(i64, i64, i64, i64)> {
    let mut watermark = 0i64;
    let mut counts: BTreeMap<(i64, i64), i64> = BTreeMap::new();
    for batch in batches {
        let mut max_ts = 0i64;
        for (key, ts, diff) in batch {
            max_ts = max_ts.max(*ts);
            if *ts < watermark {
                continue;
            }
            *counts.entry((*key, (*ts / size) * size)).or_default() += diff;
        }
        watermark = watermark.max((max_ts - lag).max(0));
    }
    counts
        .into_iter()
        .filter(|((_, ws), count)| *count != 0 && ws + size <= watermark)
        .map(|((key, ws), count)| (key, ws, count, 1))
        .collect()
}

/// Pushes batches through the operator, advancing the watermark after each one.
fn run(batches: &[Vec<(i64, i64, i64)>], size: i64, lag: i64) -> Vec<ZSetBatch> {
    let mut window = TumbleCount::new(&[0], 1, size);
    let mut watermark = Watermark::new(lag);
    let mut changelogs = Vec::new();
    for batch in batches {
        let max_ts = batch.iter().map(|r| r.1).max().unwrap_or(0);
        let wm = watermark.observe(max_ts);
        changelogs.push(window.apply(&zset(batch), wm).unwrap());
    }
    changelogs
}

#[test]
fn open_window_emits_nothing_until_watermark_reaches_end() {
    let mut window = TumbleCount::new(&[0], 1, 10);
    assert!(
        window
            .apply(&zset(&[(5, 1, 1), (5, 3, 1), (7, 7, 1)]), 7)
            .unwrap()
            .is_empty()
    );

    let out = window.apply(&zset(&[(5, 12, 1)]), 12).unwrap();
    assert_eq!(rows(&out), vec![(5, 0, 2, 1), (7, 0, 1, 1)]);
}

#[test]
fn window_closes_exactly_at_watermark_end() {
    let mut window = TumbleCount::new(&[0], 1, 10);
    window.apply(&zset(&[(5, 1, 1)]), 1).unwrap();
    let out = window.apply(&zset(&[(5, 10, 1)]), 10).unwrap();
    assert_eq!(rows(&out), vec![(5, 0, 1, 1)]);
}

#[test]
fn late_event_before_previous_watermark_is_dropped() {
    let mut window = TumbleCount::new(&[0], 1, 10);
    window.apply(&zset(&[(5, 1, 1)]), 50).unwrap();

    let out = window.apply(&zset(&[(5, 2, 1)]), 50).unwrap();
    assert!(out.is_empty());
    assert_eq!(window.late_dropped(), 1);
}

#[test]
fn retraction_before_close_adjusts_open_window() {
    let mut window = TumbleCount::new(&[0], 1, 10);
    window.apply(&zset(&[(5, 1, 1), (5, 2, 1)]), 0).unwrap();
    window.apply(&zset(&[(5, 1, -1)]), 0).unwrap();

    let out = window.apply(&zset(&[(5, 10, 1)]), 10).unwrap();
    assert_eq!(rows(&out), vec![(5, 0, 1, 1)]);
}

#[test]
fn fully_retracted_window_emits_nothing_on_close() {
    let mut window = TumbleCount::new(&[0], 1, 10);
    window.apply(&zset(&[(5, 1, 1), (5, 1, -1)]), 1).unwrap();
    let out = window.apply(&zset(&[(5, 12, 1)]), 12).unwrap();
    assert!(out.is_empty());
}

#[test]
fn closed_window_is_not_reemitted_and_is_freed() {
    let changelogs = run(
        &[
            vec![(5, 1, 1), (5, 3, 1), (5, 7, 1)],
            vec![(5, 12, 1)],
            vec![(5, 25, 1)],
        ],
        10,
        0,
    );
    assert_eq!(consolidated(&changelogs), vec![(5, 0, 3, 1), (5, 10, 1, 1)]);
}

#[test]
fn changelog_matches_full_recompute_across_windows() {
    let batches = vec![
        vec![(5, 1, 1), (5, 3, 1)],
        vec![(5, 7, 1)],
        vec![(7, 4, 1), (7, 15, 1)], // ts 4 is late against watermark 5
        vec![(7, 25, 1)],
    ];
    let changelogs = run(&batches, 10, 2);
    assert_eq!(consolidated(&changelogs), recompute(&batches, 10, 2));
    assert_eq!(rows(&changelogs[2]), vec![(5, 0, 3, 1)]);
}

#[test]
fn groups_by_key_within_the_same_window() {
    let mut window = TumbleCount::new(&[0], 1, 10);
    let out = window
        .apply(&zset(&[(5, 1, 1), (9, 2, 1), (5, 3, 1)]), 10)
        .unwrap();
    assert_eq!(rows(&out), vec![(5, 0, 2, 1), (9, 0, 1, 1)]);
}

#[test]
fn rejects_non_positive_size_and_out_of_range_columns() {
    assert!(TumbleCount::new(&[0], 1, 0).apply(&zset(&[]), 0).is_err());
    assert!(TumbleCount::new(&[0], 9, 10).apply(&zset(&[]), 0).is_err());
    assert!(TumbleCount::new(&[], 1, 10).apply(&zset(&[]), 0).is_err());
}
