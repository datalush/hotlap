//! Null-key characterization for the cross-source inner join.
//!
//! The engine encodes join keys through `arrow::row`, so a NULL key compares
//! equal to another NULL. This test records that existing semantics without
//! changing it: the independent oracle treats `None == None` as a match and
//! never silently drops a null-key row. The SQL null rules are untouched.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use hotlap_core::{IncrementalCore, InputId, Plan, ViewId, ZSetBatch};
use hotlap_engine::EngineCore;

/// An input row `(key, value, diff)` with a nullable key.
type InputRow = (Option<i64>, i64, i64);

/// A join row `(key, left, right, diff)` with a nullable key.
type JoinedRow = (Option<i64>, i64, i64, i64);

/// Nullable `(k, v)` schema: `k` is the join key and may be null.
fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("v", DataType::Int64, false),
    ]))
}

/// One delta of `(key, value)` rows with signed diffs.
fn delta(rows: &[(Option<i64>, i64)], diffs: &[i64]) -> ZSetBatch {
    let keys: Vec<Option<i64>> = rows.iter().map(|row| row.0).collect();
    let values: Vec<i64> = rows.iter().map(|row| row.1).collect();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(keys)),
        Arc::new(Int64Array::from(values)),
    ];
    let batch = RecordBatch::try_new(schema(), columns).unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs.to_vec()))).unwrap()
}

/// Oracle: `None == None` matches, because the engine compares encoded rows.
fn recompute(left: &[InputRow], right: &[InputRow]) -> Vec<JoinedRow> {
    let mut rows: BTreeMap<(Option<i64>, i64, i64), i64> = BTreeMap::new();
    for &(lk, lv, ld) in left {
        for &(rk, rv, rd) in right {
            if lk == rk {
                *rows.entry((lk, lv, rv)).or_insert(0) += ld * rd;
            }
        }
    }
    let mut out: Vec<JoinedRow> = rows
        .into_iter()
        .filter(|(_, diff)| *diff != 0)
        .map(|((k, l, r), diff)| (k, l, r, diff))
        .collect();
    out.sort_unstable();
    out
}

fn key_at(keys: &Int64Array, row: usize) -> Option<i64> {
    (!keys.is_null(row)).then(|| keys.value(row))
}

/// Snapshot as `(key, left, right, diff)`, with `None` for a null key.
fn snapshot_rows(zset: &ZSetBatch) -> Vec<JoinedRow> {
    if zset.is_empty() {
        return Vec::new();
    }
    let keys = zset
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let left = zset
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let right = zset
        .batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let diffs = zset.diff.as_any().downcast_ref::<Int64Array>().unwrap();
    let mut out: Vec<JoinedRow> = (0..zset.len())
        .map(|row| {
            (
                key_at(keys, row),
                left.value(row),
                right.value(row),
                diffs.value(row),
            )
        })
        .collect();
    out.sort_unstable();
    out
}

fn join_core() -> EngineCore {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.register_input(InputId(1)).unwrap();
    core.build_view(
        ViewId(0),
        &Plan::Join {
            left: Box::new(Plan::Source(InputId(0))),
            right: Box::new(Plan::Source(InputId(1))),
            left_key: vec![0],
            right_key: vec![0],
        },
    )
    .unwrap();
    core
}

#[test]
fn null_keys_match_each_other_and_are_not_dropped() {
    let mut core = join_core();
    let (mut left, mut right): (Vec<InputRow>, Vec<InputRow>) = (Vec::new(), Vec::new());

    left.extend([(Some(1), 10, 1), (None, 11, 1)]);
    core.push(InputId(0), &delta(&[(Some(1), 10), (None, 11)], &[1, 1]))
        .unwrap();
    assert!(snapshot_rows(&core.snapshot(ViewId(0)).unwrap()).is_empty());

    right.extend([(Some(1), 20, 2), (None, 21, 1), (Some(3), 22, 1)]);
    core.push(
        InputId(1),
        &delta(&[(Some(1), 20), (None, 21), (Some(3), 22)], &[2, 1, 1]),
    )
    .unwrap();
    let snapshot = core.snapshot(ViewId(0)).unwrap();
    assert_eq!(snapshot_rows(&snapshot), recompute(&left, &right));
    assert_eq!(
        snapshot_rows(&snapshot),
        vec![(None, 11, 21, 1), (Some(1), 10, 20, 2)],
        "a null key must match a null key, not be dropped"
    );

    // Retracting the null-key left row removes its join pair.
    left.push((None, 11, -1));
    core.push(InputId(0), &delta(&[(None, 11)], &[-1])).unwrap();
    let snapshot = core.snapshot(ViewId(0)).unwrap();
    assert_eq!(snapshot_rows(&snapshot), recompute(&left, &right));
    assert_eq!(snapshot_rows(&snapshot), vec![(Some(1), 10, 20, 2)]);

    // Retracting the matching positive key empties the join.
    right.push((Some(1), 20, -2));
    core.push(InputId(1), &delta(&[(Some(1), 20)], &[-2]))
        .unwrap();
    assert!(snapshot_rows(&core.snapshot(ViewId(0)).unwrap()).is_empty());
}
