//! A rejected retraction must not leave partially mutated aggregate state.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use hotlap_core::plan::AggSpec;

use super::GroupAggregate;
use crate::batch::ZSetBatch;

/// One `(key, a, b, diff)` row over two `Float64` value columns.
fn zset(rows: &[(i64, f64, f64, i64)]) -> ZSetBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("a", DataType::Float64, true),
        Field::new("b", DataType::Float64, true),
    ]));
    let keys: Vec<i64> = rows.iter().map(|row| row.0).collect();
    let a: Vec<f64> = rows.iter().map(|row| row.1).collect();
    let b: Vec<f64> = rows.iter().map(|row| row.2).collect();
    let diffs: Vec<i64> = rows.iter().map(|row| row.3).collect();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(keys)),
        Arc::new(Float64Array::from(a)),
        Arc::new(Float64Array::from(b)),
    ];
    let batch = RecordBatch::try_new(schema, columns).unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs))).unwrap()
}

/// `(key, first, second, diff)` output cells, sorted old-before-new.
fn cells(z: &ZSetBatch) -> Vec<(i64, Option<f64>, Option<f64>, i64)> {
    let key = z
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let cast = |index: usize| {
        z.batch
            .column(index)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .clone()
    };
    let (first, second) = (cast(1), cast(2));
    let diffs = z.diff.as_any().downcast_ref::<Int64Array>().unwrap();
    let cell = |array: &Float64Array, i: usize| (!array.is_null(i)).then(|| array.value(i));
    let mut out: Vec<_> = (0..z.len())
        .map(|i| {
            (
                key.value(i),
                cell(&first, i),
                cell(&second, i),
                diffs.value(i),
            )
        })
        .collect();
    out.sort_by_key(|row| (row.0, row.3));
    out
}

#[test]
fn rejected_special_retraction_preserves_prior_sum() {
    let mut reducer = GroupAggregate::new(&[0], vec![AggSpec::sum(1), AggSpec::avg(1)]);
    reducer.apply(&zset(&[(1, 2.0, 0.0, 1)])).unwrap();

    assert!(
        reducer.apply(&zset(&[(1, f64::NAN, 0.0, -1)])).is_err(),
        "over-retracting a special must be rejected"
    );

    // The surviving state must still be `sum = avg = 2`, so retracting 2
    // removes the group and emits only the prior row.
    let out = reducer.apply(&zset(&[(1, 2.0, 0.0, -1)])).unwrap();
    assert_eq!(cells(&out), vec![(1, Some(2.0), Some(2.0), -1)]);
}

#[test]
fn later_special_error_leaves_earlier_sum_slot_unchanged() {
    let mut reducer = GroupAggregate::new(&[0], vec![AggSpec::sum(1), AggSpec::sum(2)]);
    reducer.apply(&zset(&[(1, 5.0, 0.0, 1)])).unwrap();

    // The first row grows `sum(a)`; the second over-retracts `NaN` on `b`.
    assert!(
        reducer
            .apply(&zset(&[(1, 7.0, 0.0, 1), (1, 0.0, f64::NAN, -1)]))
            .is_err()
    );

    let out = reducer.apply(&zset(&[(1, 5.0, 0.0, -1)])).unwrap();
    assert_eq!(cells(&out), vec![(1, Some(5.0), Some(0.0), -1)]);
}

#[test]
fn later_special_error_leaves_earlier_min_unchanged() {
    let mut reducer = GroupAggregate::new(&[0], vec![AggSpec::min(1), AggSpec::sum(2)]);
    reducer.apply(&zset(&[(1, 5.0, 0.0, 1)])).unwrap();

    // The first row lowers `min(a)`; the second over-retracts `NaN` on `b`.
    assert!(
        reducer
            .apply(&zset(&[(1, 3.0, 0.0, 1), (1, 0.0, f64::NAN, -1)]))
            .is_err()
    );

    let out = reducer.apply(&zset(&[(1, 5.0, 0.0, -1)])).unwrap();
    assert_eq!(cells(&out), vec![(1, Some(5.0), Some(0.0), -1)]);
}
