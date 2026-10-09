//! Full-recomputation oracle for cross-source inner joins.
//!
//! The oracle never calls the incremental join: it recombines the raw input rows
//! and multiplicities after every delta.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use hotlap_core::{IncrementalCore, InputId, Plan, ViewId, ZSetBatch};
use hotlap_engine::EngineCore;

/// Full recompute: inputs are `[key..., value, diff]`, output is
/// `[key..., left, right, diff]`, consolidated across duplicate pairs.
fn recompute(left: &[Vec<i64>], right: &[Vec<i64>], keys: usize) -> Vec<Vec<i64>> {
    let mut rows: BTreeMap<Vec<i64>, i64> = BTreeMap::new();
    for l in left {
        for r in right {
            if l[..keys] == r[..keys] {
                let mut key = l[..keys].to_vec();
                key.push(l[keys]);
                key.push(r[keys]);
                *rows.entry(key).or_insert(0) += l[keys + 1] * r[keys + 1];
            }
        }
    }
    rows.into_iter()
        .filter(|(_, diff)| *diff != 0)
        .map(|(mut key, diff)| {
            key.push(diff);
            key
        })
        .collect()
}

#[test]
fn oracle_consolidates_duplicate_pairs() {
    let left = [vec![1, 10, 2]];
    let right = [vec![1, 20, 3]];
    assert_eq!(recompute(&left, &right, 1), vec![vec![1, 10, 20, 6]]);
    assert!(recompute(&[vec![1, 10, 1], vec![1, 10, -1]], &right, 1).is_empty());
}

fn schema(fields: &[&str]) -> SchemaRef {
    Arc::new(Schema::new(
        fields
            .iter()
            .map(|name| Field::new(*name, DataType::Int64, false))
            .collect::<Vec<_>>(),
    ))
}

fn batch(schema: &SchemaRef, rows: &[Vec<i64>], diffs: &[i64]) -> ZSetBatch {
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

fn delta(schema: &SchemaRef, row: Vec<i64>, diff: i64) -> ZSetBatch {
    batch(schema, &[row], &[diff])
}

fn column_values(zset: &ZSetBatch, column: usize) -> Vec<i64> {
    let array = zset
        .batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    (0..zset.len()).map(|row| array.value(row)).collect()
}

fn diff_values(zset: &ZSetBatch) -> Vec<i64> {
    let array = zset.diff.as_any().downcast_ref::<Int64Array>().unwrap();
    (0..zset.len()).map(|row| array.value(row)).collect()
}

/// Snapshot rows `[key..., left, right, diff]` from a join over `keys` columns.
fn joined_rows(zset: &ZSetBatch, keys: usize) -> Vec<Vec<i64>> {
    if zset.is_empty() {
        return Vec::new();
    }
    let columns: Vec<Vec<i64>> = (0..zset.batch.num_columns())
        .map(|column| column_values(zset, column))
        .collect();
    let diffs = diff_values(zset);
    let mut rows: Vec<Vec<i64>> = (0..zset.len())
        .map(|row| {
            let mut out: Vec<i64> = (0..keys).map(|key| columns[key][row]).collect();
            out.push(columns[keys][row]);
            out.push(columns[2 * keys + 1][row]);
            out.push(diffs[row]);
            out
        })
        .collect();
    rows.sort();
    rows
}

fn join_core(left_key: Vec<usize>, right_key: Vec<usize>) -> EngineCore {
    let mut core = EngineCore::new();
    core.register_input(InputId(0)).unwrap();
    core.register_input(InputId(1)).unwrap();
    core.build_view(
        ViewId(0),
        &Plan::Join {
            left: Box::new(Plan::Source(InputId(0))),
            right: Box::new(Plan::Source(InputId(1))),
            left_key,
            right_key,
        },
    )
    .unwrap();
    core
}

#[test]
fn single_key_join_matches_recomputation_after_every_delta() {
    let schema = schema(&["k", "v"]);
    let mut core = join_core(vec![0], vec![0]);
    let (mut left, mut right): (Vec<Vec<i64>>, Vec<Vec<i64>>) = (Vec::new(), Vec::new());
    let deltas: [(u32, i64, i64, i64); 6] = [
        (0, 1, 10, 2),
        (1, 1, 20, 3),
        (0, 1, 10, -1),
        (1, 2, 30, 1),
        (1, 1, 20, -3),
        (0, 2, 40, 1),
    ];
    for (side, k, v, diff) in deltas {
        let row = vec![k, v, diff];
        if side == 0 {
            left.push(row);
        } else {
            right.push(row);
        }
        core.push(InputId(side), &delta(&schema, vec![k, v], diff))
            .unwrap();
        let snapshot = core.snapshot(ViewId(0)).unwrap();
        assert_eq!(joined_rows(&snapshot, 1), recompute(&left, &right, 1));
    }
}

#[test]
fn composite_key_join_matches_recomputation_after_every_delta() {
    let schema = schema(&["k1", "k2", "v"]);
    let mut core = join_core(vec![0, 1], vec![0, 1]);
    let (mut left, mut right): (Vec<Vec<i64>>, Vec<Vec<i64>>) = (Vec::new(), Vec::new());
    let deltas: [(u32, i64, i64, i64, i64); 7] = [
        (0, 1, 1, 10, 1),
        (1, 1, 1, 20, 2),
        (0, 1, 2, 11, 1),
        (1, 1, 2, 21, 1),
        (0, 1, 1, 10, -1),
        (1, 2, 2, 30, 1),
        (0, 1, 2, 11, 1),
    ];
    for (side, k1, k2, v, diff) in deltas {
        let row = vec![k1, k2, v, diff];
        if side == 0 {
            left.push(row);
        } else {
            right.push(row);
        }
        core.push(InputId(side), &delta(&schema, vec![k1, k2, v], diff))
            .unwrap();
        let snapshot = core.snapshot(ViewId(0)).unwrap();
        assert_eq!(joined_rows(&snapshot, 2), recompute(&left, &right, 2));
    }

    // The same deltas in reverse push order, checked after every delta against
    // an independently accumulated oracle (not the forward final state).
    let (mut rev_left, mut rev_right): (Vec<Vec<i64>>, Vec<Vec<i64>>) = (Vec::new(), Vec::new());
    let mut reversed = join_core(vec![0, 1], vec![0, 1]);
    for (side, k1, k2, v, diff) in deltas.iter().rev() {
        if *side == 0 {
            rev_left.push(vec![*k1, *k2, *v, *diff]);
        } else {
            rev_right.push(vec![*k1, *k2, *v, *diff]);
        }
        reversed
            .push(InputId(*side), &delta(&schema, vec![*k1, *k2, *v], *diff))
            .unwrap();
        let snapshot = reversed.snapshot(ViewId(0)).unwrap();
        assert_eq!(
            joined_rows(&snapshot, 2),
            recompute(&rev_left, &rev_right, 2)
        );
    }
}
