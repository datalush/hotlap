//! Unit tests for the incremental grouped-aggregate reducer.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use hotlap_core::plan::AggSpec;

use super::GroupAggregate;
use crate::batch::ZSetBatch;

fn zset(column: &str, values: &[(i64, i64)], diffs: &[i64]) -> ZSetBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new(column, DataType::Int64, true),
    ]));
    let keys: Vec<i64> = values.iter().map(|row| row.0).collect();
    let vals: Vec<i64> = values.iter().map(|row| row.1).collect();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(keys)),
        Arc::new(Int64Array::from(vals)),
    ];
    let batch = RecordBatch::try_new(schema, columns).unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs.to_vec()))).unwrap()
}

/// `(key, count, sum, avg, diff)` rows sorted for comparison.
fn rows(z: &ZSetBatch) -> Vec<(i64, i64, i64, f64, i64)> {
    let key = int_column(z, 0);
    let count = int_column(z, 1);
    let sum = int_column(z, 2);
    let avg = z
        .batch
        .column(3)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let diffs = z.diff.as_any().downcast_ref::<Int64Array>().unwrap();
    let mut out: Vec<(i64, i64, i64, f64, i64)> = (0..z.len())
        .map(|i| {
            (
                key.value(i),
                count.value(i),
                sum.value(i),
                avg.value(i),
                diffs.value(i),
            )
        })
        .collect();
    out.sort_by(|a, b| a.partial_cmp(b).unwrap());
    out
}

fn int_column(z: &ZSetBatch, index: usize) -> Int64Array {
    z.batch
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .clone()
}

fn reducer() -> GroupAggregate {
    GroupAggregate::new(
        &[0],
        vec![AggSpec::count(), AggSpec::sum(1), AggSpec::avg(1)],
    )
}

#[test]
fn insert_then_retract_updates_all_aggregates() {
    let mut reducer = reducer();
    let first = reducer.apply(&zset("v", &[(1, 10)], &[1])).unwrap();
    assert_eq!(rows(&first), vec![(1, 1, 10, 10.0, 1)]);

    let second = reducer.apply(&zset("v", &[(1, 20)], &[1])).unwrap();
    assert_eq!(
        rows(&second),
        vec![(1, 1, 10, 10.0, -1), (1, 2, 30, 15.0, 1)]
    );
}

#[test]
fn retraction_lowers_sum_avg_and_count() {
    let mut reducer = reducer();
    reducer
        .apply(&zset("v", &[(1, 10), (1, 20)], &[1, 1]))
        .unwrap();
    let out = reducer.apply(&zset("v", &[(1, 20)], &[-1])).unwrap();
    assert_eq!(rows(&out), vec![(1, 1, 10, 10.0, 1), (1, 2, 30, 15.0, -1)]);
}

#[test]
fn retraction_to_zero_retracts_the_group() {
    let mut reducer = reducer();
    reducer.apply(&zset("v", &[(1, 10)], &[1])).unwrap();
    let out = reducer.apply(&zset("v", &[(1, 10)], &[-1])).unwrap();
    assert_eq!(rows(&out), vec![(1, 1, 10, 10.0, -1)]);
    assert_eq!(reducer.work(), 1);
}

#[test]
fn swaps_that_keep_the_row_count_still_update_sum() {
    let mut reducer = reducer();
    reducer.apply(&zset("v", &[(1, 10)], &[1])).unwrap();
    // Retract 10 and insert 30 in one delta: the row count is unchanged.
    let out = reducer
        .apply(&zset("v", &[(1, 10), (1, 30)], &[-1, 1]))
        .unwrap();
    assert_eq!(rows(&out), vec![(1, 1, 10, 10.0, -1), (1, 1, 30, 30.0, 1)]);
}

#[test]
fn float_sum_and_avg_are_tracked() {
    let mut reducer = GroupAggregate::new(&[0], vec![AggSpec::sum(1), AggSpec::avg(1)]);
    let out = reducer.apply(&zset("v", &[(1, 4)], &[1])).unwrap();
    assert_eq!(out.schema().field(1).data_type(), &DataType::Int64);
    assert_eq!(out.schema().field(2).data_type(), &DataType::Float64);
}

#[test]
fn snapshot_round_trips_aggregate_state() {
    let mut source = reducer();
    source
        .apply(&zset("v", &[(1, 10), (1, 20), (2, 5)], &[1, 1, 1]))
        .unwrap();
    let state = source.export_state().unwrap();

    let mut restored = reducer();
    restored.import_state(&state).unwrap();
    let out = restored.apply(&zset("v", &[(1, 30)], &[1])).unwrap();
    assert_eq!(rows(&out), vec![(1, 2, 30, 15.0, -1), (1, 3, 60, 20.0, 1)]);
}

#[test]
fn work_is_proportional_to_touched_keys() {
    let mut reducer = reducer();
    for key in 0..20i64 {
        let out = reducer.apply(&zset("v", &[(key, 1)], &[1])).unwrap();
        assert_eq!(reducer.work(), 1);
        assert_eq!(rows(&out), vec![(key, 1, 1, 1.0, 1)]);
    }
}
