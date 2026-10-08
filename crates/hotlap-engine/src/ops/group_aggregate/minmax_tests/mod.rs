//! Unit tests for retraction-aware `min`/`max` aggregates.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Int32Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use hotlap_core::plan::AggSpec;

use super::GroupAggregate;
use crate::batch::ZSetBatch;

mod types;

/// Builds a Z-set with an `Int64` key column and the given value column.
fn zset(keys: &[i64], values: ArrayRef, diffs: &[i64]) -> ZSetBatch {
    let value_type = values.data_type().clone();
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", value_type, true),
    ]));
    let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(keys.to_vec())), values];
    let batch = RecordBatch::try_new(schema, columns).unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs.to_vec()))).unwrap()
}

fn int64_values(values: &[Option<i64>]) -> ArrayRef {
    Arc::new(Int64Array::from(values.to_vec()))
}

fn int32_values(values: &[Option<i32>]) -> ArrayRef {
    Arc::new(Int32Array::from(values.to_vec()))
}

fn float_values(values: &[Option<f64>]) -> ArrayRef {
    Arc::new(Float64Array::from(values.to_vec()))
}

fn int_column(z: &ZSetBatch, index: usize) -> Int64Array {
    z.batch
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .clone()
}

fn int_cell(array: &Int64Array, row: usize) -> Option<i64> {
    if array.is_null(row) {
        None
    } else {
        Some(array.value(row))
    }
}

/// `(key, min, max, diff)` rows sorted for comparison.
fn int_rows(z: &ZSetBatch) -> Vec<(i64, Option<i64>, Option<i64>, i64)> {
    let key = int_column(z, 0);
    let min = int_column(z, 1);
    let max = int_column(z, 2);
    let diffs = z.diff.as_any().downcast_ref::<Int64Array>().unwrap();
    let mut out: Vec<_> = (0..z.len())
        .map(|i| {
            (
                key.value(i),
                int_cell(&min, i),
                int_cell(&max, i),
                diffs.value(i),
            )
        })
        .collect();
    out.sort();
    out
}

/// `(key, min, max, diff)` rows over `Float64` aggregates.
fn float_rows(z: &ZSetBatch) -> Vec<(i64, Option<f64>, Option<f64>, i64)> {
    let key = int_column(z, 0);
    let cast = |index: usize| {
        z.batch
            .column(index)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .clone()
    };
    let (min, max) = (cast(1), cast(2));
    let diffs = z.diff.as_any().downcast_ref::<Int64Array>().unwrap();
    let cell = |a: &Float64Array, i: usize| (!a.is_null(i)).then(|| a.value(i));
    (0..z.len())
        .map(|i| (key.value(i), cell(&min, i), cell(&max, i), diffs.value(i)))
        .collect()
}

fn reducer() -> GroupAggregate {
    GroupAggregate::new(&[0], vec![AggSpec::min(1), AggSpec::max(1)])
}

#[test]
fn tracks_min_and_max_per_key() {
    let mut reducer = reducer();
    let out = reducer
        .apply(&zset(
            &[1, 1, 2],
            int64_values(&[Some(20), Some(10), Some(7)]),
            &[1, 1, 1],
        ))
        .unwrap();
    assert_eq!(
        int_rows(&out),
        vec![(1, Some(10), Some(20), 1), (2, Some(7), Some(7), 1)]
    );
}

#[test]
fn retracting_current_min_recomputes_the_next() {
    let mut reducer = reducer();
    reducer
        .apply(&zset(
            &[1, 1, 1],
            int64_values(&[Some(10), Some(20), Some(30)]),
            &[1, 1, 1],
        ))
        .unwrap();
    let out = reducer
        .apply(&zset(&[1], int64_values(&[Some(10)]), &[-1]))
        .unwrap();
    assert_eq!(
        int_rows(&out),
        vec![(1, Some(10), Some(30), -1), (1, Some(20), Some(30), 1)]
    );
}

#[test]
fn retracting_current_max_recomputes_the_next() {
    let mut reducer = reducer();
    reducer
        .apply(&zset(
            &[1, 1, 1],
            int64_values(&[Some(10), Some(20), Some(30)]),
            &[1, 1, 1],
        ))
        .unwrap();
    let out = reducer
        .apply(&zset(&[1], int64_values(&[Some(30)]), &[-1]))
        .unwrap();
    assert_eq!(
        int_rows(&out),
        vec![(1, Some(10), Some(20), 1), (1, Some(10), Some(30), -1)]
    );
}

#[test]
fn retracting_a_non_extreme_value_emits_nothing() {
    let mut reducer = reducer();
    reducer
        .apply(&zset(
            &[1, 1, 1],
            int64_values(&[Some(10), Some(20), Some(30)]),
            &[1, 1, 1],
        ))
        .unwrap();
    let out = reducer
        .apply(&zset(&[1], int64_values(&[Some(20)]), &[-1]))
        .unwrap();
    assert!(out.is_empty(), "the visible extremes did not change");
}

#[test]
fn emptying_a_key_retracts_the_group() {
    let mut reducer = reducer();
    reducer
        .apply(&zset(&[1], int64_values(&[Some(10)]), &[1]))
        .unwrap();
    let out = reducer
        .apply(&zset(&[1], int64_values(&[Some(10)]), &[-1]))
        .unwrap();
    assert_eq!(int_rows(&out), vec![(1, Some(10), Some(10), -1)]);
}

#[test]
fn nulls_are_ignored() {
    let mut reducer = reducer();
    let out = reducer
        .apply(&zset(
            &[1, 1, 1],
            int64_values(&[Some(5), None, Some(8)]),
            &[1, 1, 1],
        ))
        .unwrap();
    assert_eq!(int_rows(&out), vec![(1, Some(5), Some(8), 1)]);
}
