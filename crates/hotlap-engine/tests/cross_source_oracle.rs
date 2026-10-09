//! Full-recomputation oracle for cross-source inner joins.
//!
//! The oracle never calls the incremental join: it recomputes the equi-join
//! from the raw input rows and their multiplicities after every delta, so a
//! passing comparison is independent evidence of the incremental result.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use hotlap_core::{IncrementalCore, InputId, Plan, ViewId, ZSetBatch};
use hotlap_engine::EngineCore;

/// Full recompute of `left JOIN right ON k` with insert/retract multiplicities.
fn recompute(left: &[(i64, i64, i64)], right: &[(i64, i64, i64)]) -> Vec<(i64, i64, i64, i64)> {
    let mut rows: BTreeMap<(i64, i64, i64), i64> = BTreeMap::new();
    for &(lk, lv, ld) in left {
        for &(rk, rv, rd) in right {
            if lk == rk {
                *rows.entry((lk, lv, rv)).or_insert(0) += ld * rd;
            }
        }
    }
    rows.into_iter()
        .filter(|(_, diff)| *diff != 0)
        .map(|((k, l, r), diff)| (k, l, r, diff))
        .collect()
}

/// Full recompute for composite keys `(k1, k2)`; rows are `(k1, k2, v, diff)`.
fn recompute_composite(
    left: &[(i64, i64, i64, i64)],
    right: &[(i64, i64, i64, i64)],
) -> Vec<(i64, i64, i64, i64, i64)> {
    let mut rows: BTreeMap<(i64, i64, i64, i64), i64> = BTreeMap::new();
    for &(lk1, lk2, lv, ld) in left {
        for &(rk1, rk2, rv, rd) in right {
            if lk1 == rk1 && lk2 == rk2 {
                *rows.entry((lk1, lk2, lv, rv)).or_insert(0) += ld * rd;
            }
        }
    }
    rows.into_iter()
        .filter(|(_, diff)| *diff != 0)
        .map(|((k1, k2, l, r), diff)| (k1, k2, l, r, diff))
        .collect()
}

#[test]
fn oracle_consolidates_duplicate_pairs() {
    assert_eq!(
        recompute(&[(1, 10, 2)], &[(1, 20, 3)]),
        vec![(1, 10, 20, 6)]
    );
    assert!(recompute(&[(1, 10, 1), (1, 10, -1)], &[(1, 20, 1)]).is_empty());
}

fn schema(fields: &[&str]) -> SchemaRef {
    Arc::new(Schema::new(
        fields
            .iter()
            .map(|name| Field::new(*name, DataType::Int64, false))
            .collect::<Vec<_>>(),
    ))
}

/// Builds a Z-set over `schema` from `rows` and their signed diffs.
fn zset(schema: &SchemaRef, rows: &[Vec<i64>], diffs: &[i64]) -> ZSetBatch {
    let columns: Vec<ArrayRef> = (0..schema.fields().len())
        .map(|column| -> ArrayRef {
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row[column]).collect::<Vec<_>>(),
            ))
        })
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs.to_vec()))).unwrap()
}

fn single_delta(k: i64, v: i64, diff: i64) -> ZSetBatch {
    zset(&schema(&["k", "v"]), &[vec![k, v]], &[diff])
}

fn composite_delta(k1: i64, k2: i64, v: i64, diff: i64) -> ZSetBatch {
    zset(&schema(&["k1", "k2", "v"]), &[vec![k1, k2, v]], &[diff])
}

fn column(zset: &ZSetBatch, column: usize) -> Vec<i64> {
    let array = zset
        .batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (0..zset.len()).map(|row| array.value(row)).collect()
}

fn diffs(zset: &ZSetBatch) -> Vec<i64> {
    let array = zset.diff.as_any().downcast_ref::<Int64Array>().unwrap();
    (0..zset.len()).map(|row| array.value(row)).collect()
}

/// Snapshot of a `(k, v)` join as sorted `(key, left, right, diff)` tuples.
fn single_rows(zset: &ZSetBatch) -> Vec<(i64, i64, i64, i64)> {
    if zset.is_empty() {
        return Vec::new();
    }
    let (keys, left) = (column(zset, 0), column(zset, 1));
    let right = column(zset, 3);
    let diffs = diffs(zset);
    (0..zset.len())
        .map(|row| (keys[row], left[row], right[row], diffs[row]))
        .collect()
}

/// Snapshot of a `(k1, k2, v)` join as sorted `(k1, k2, left, right, diff)`.
fn composite_rows(zset: &ZSetBatch) -> Vec<(i64, i64, i64, i64, i64)> {
    if zset.is_empty() {
        return Vec::new();
    }
    let (k1, k2, left) = (column(zset, 0), column(zset, 1), column(zset, 2));
    let right = column(zset, 5);
    let diffs = diffs(zset);
    (0..zset.len())
        .map(|row| (k1[row], k2[row], left[row], right[row], diffs[row]))
        .collect()
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
fn single_key_join_matches_recomputation_after_every_delta() {
    let mut core = join_core();
    let mut left: Vec<(i64, i64, i64)> = Vec::new();
    let mut right: Vec<(i64, i64, i64)> = Vec::new();
    let deltas = [
        (0, 1, 10, 2),
        (1, 1, 20, 3),
        (0, 1, 10, -1),
        (1, 2, 30, 1),
        (1, 1, 20, -3),
        (0, 2, 40, 1),
    ];
    for (side, k, v, diff) in deltas {
        let batch = single_delta(k, v, diff);
        if side == 0 {
            left.push((k, v, diff));
            core.push(InputId(0), &batch).unwrap();
        } else {
            right.push((k, v, diff));
            core.push(InputId(1), &batch).unwrap();
        }
        assert_eq!(
            single_rows(&core.snapshot(ViewId(0)).unwrap()),
            recompute(&left, &right)
        );
    }
}

fn composite_core() -> EngineCore {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.register_input(InputId(1)).unwrap();
    core.build_view(
        ViewId(0),
        &Plan::Join {
            left: Box::new(Plan::Source(InputId(0))),
            right: Box::new(Plan::Source(InputId(1))),
            left_key: vec![0, 1],
            right_key: vec![0, 1],
        },
    )
    .unwrap();
    core
}

#[test]
fn composite_key_join_matches_recomputation_after_every_delta() {
    let mut core = composite_core();
    let mut left: Vec<(i64, i64, i64, i64)> = Vec::new();
    let mut right: Vec<(i64, i64, i64, i64)> = Vec::new();
    let deltas = [
        (0, 1, 1, 10, 1),
        (1, 1, 1, 20, 2),
        (0, 1, 2, 11, 1),
        (1, 1, 2, 21, 1),
        (0, 1, 1, 10, -1),
        (1, 2, 2, 30, 1),
        (0, 1, 2, 11, 1),
    ];
    for (side, k1, k2, v, diff) in deltas {
        let batch = composite_delta(k1, k2, v, diff);
        if side == 0 {
            left.push((k1, k2, v, diff));
            core.push(InputId(0), &batch).unwrap();
        } else {
            right.push((k1, k2, v, diff));
            core.push(InputId(1), &batch).unwrap();
        }
        assert_eq!(
            composite_rows(&core.snapshot(ViewId(0)).unwrap()),
            recompute_composite(&left, &right)
        );
    }
}

#[test]
fn composite_join_is_independent_of_push_order() {
    let forward = [(0, 1, 1, 10, 1), (1, 1, 1, 20, 2), (0, 1, 2, 11, 1)];
    let reversed = [(0, 1, 2, 11, 1), (1, 1, 1, 20, 2), (0, 1, 1, 10, 1)];

    let mut first = composite_core();
    let mut second = composite_core();
    for (side, k1, k2, v, diff) in forward {
        let batch = composite_delta(k1, k2, v, diff);
        first
            .push(InputId(if side == 0 { 0 } else { 1 }), &batch)
            .unwrap();
    }
    for (side, k1, k2, v, diff) in reversed {
        let batch = composite_delta(k1, k2, v, diff);
        second
            .push(InputId(if side == 0 { 0 } else { 1 }), &batch)
            .unwrap();
    }

    let left = [(1, 1, 10, 1), (1, 2, 11, 1)];
    let right = [(1, 1, 20, 2)];
    let expected = recompute_composite(&left, &right);
    let first_rows = composite_rows(&first.snapshot(ViewId(0)).unwrap());
    assert_eq!(first_rows, expected);
    assert_eq!(
        composite_rows(&second.snapshot(ViewId(0)).unwrap()),
        expected
    );
}
