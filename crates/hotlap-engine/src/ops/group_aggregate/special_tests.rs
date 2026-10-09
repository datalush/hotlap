//! Retraction of `NaN` and infinite float inputs from `sum`/`avg`.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use hotlap_core::plan::AggSpec;

use super::GroupAggregate;
use crate::batch::ZSetBatch;

/// One `(key, value, diff)` row over an `Int64` key and a `Float64` value.
fn zset(rows: &[(i64, f64, i64)]) -> ZSetBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Float64, true),
    ]));
    let keys: Vec<i64> = rows.iter().map(|r| r.0).collect();
    let values: Vec<f64> = rows.iter().map(|r| r.1).collect();
    let diffs: Vec<i64> = rows.iter().map(|r| r.2).collect();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(keys)),
        Arc::new(Float64Array::from(values)),
    ];
    let batch = RecordBatch::try_new(schema, columns).unwrap();
    ZSetBatch::new(batch, Arc::new(Int64Array::from(diffs))).unwrap()
}

/// The materialized `(key, sum, avg)` of each inserted (`diff == 1`) output row.
fn current(z: &ZSetBatch) -> Vec<(i64, Option<f64>, Option<f64>)> {
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
    let (sum, avg) = (cast(1), cast(2));
    let diffs = z.diff.as_any().downcast_ref::<Int64Array>().unwrap();
    let cell = |a: &Float64Array, i: usize| (!a.is_null(i)).then(|| a.value(i));
    (0..z.len())
        .filter(|&i| diffs.value(i) == 1)
        .map(|i| (key.value(i), cell(&sum, i), cell(&avg, i)))
        .collect()
}

/// Asserts the sole current row's `sum` and `avg` cells are both `NaN`.
fn assert_current_is_nan(z: &ZSetBatch) {
    let current = current(z);
    assert_eq!(current.len(), 1, "expected one inserted row: {current:?}");
    assert_eq!(current[0].1.map(f64::is_nan), Some(true), "{current:?}");
    assert_eq!(current[0].2.map(f64::is_nan), Some(true), "{current:?}");
}

fn reducer() -> GroupAggregate {
    GroupAggregate::new(&[0], vec![AggSpec::sum(1), AggSpec::avg(1)])
}

#[test]
fn retracting_nan_recovers_the_finite_sum_and_avg() {
    let mut reducer = reducer();
    reducer
        .apply(&zset(&[(1, f64::NAN, 1), (1, 2.0, 1)]))
        .unwrap();
    let out = reducer.apply(&zset(&[(1, f64::NAN, -1)])).unwrap();
    assert_eq!(current(&out), vec![(1, Some(2.0), Some(2.0))]);
}

#[test]
fn retracting_positive_infinity_recovers_the_finite_sum() {
    let mut reducer = reducer();
    reducer
        .apply(&zset(&[(1, f64::INFINITY, 1), (1, 3.0, 1)]))
        .unwrap();
    let out = reducer.apply(&zset(&[(1, f64::INFINITY, -1)])).unwrap();
    assert_eq!(current(&out), vec![(1, Some(3.0), Some(3.0))]);
}

#[test]
fn retracting_negative_infinity_recovers_the_finite_sum() {
    let mut reducer = reducer();
    reducer
        .apply(&zset(&[(1, f64::NEG_INFINITY, 1), (1, 3.0, 1)]))
        .unwrap();
    let out = reducer.apply(&zset(&[(1, f64::NEG_INFINITY, -1)])).unwrap();
    assert_eq!(current(&out), vec![(1, Some(3.0), Some(3.0))]);
}

#[test]
fn opposite_infinities_render_nan_then_recover_step_by_step() {
    let mut reducer = reducer();
    let mixed = reducer
        .apply(&zset(&[
            (1, f64::INFINITY, 1),
            (1, f64::NEG_INFINITY, 1),
            (1, 4.0, 1),
        ]))
        .unwrap();
    assert_current_is_nan(&mixed);

    // Removing one infinite side leaves the other.
    let pos = reducer.apply(&zset(&[(1, f64::NEG_INFINITY, -1)])).unwrap();
    assert_eq!(
        current(&pos),
        vec![(1, Some(f64::INFINITY), Some(f64::INFINITY))]
    );

    // Removing the last infinite side recovers the finite sum.
    let finite = reducer.apply(&zset(&[(1, f64::INFINITY, -1)])).unwrap();
    assert_eq!(current(&finite), vec![(1, Some(4.0), Some(4.0))]);
}

#[test]
fn weighted_special_retractions_recover_correctly() {
    let mut reducer = reducer();
    let initial = reducer
        .apply(&zset(&[
            (1, f64::NAN, 1),
            (1, f64::INFINITY, 2),
            (1, 3.0, 1),
        ]))
        .unwrap();
    assert_current_is_nan(&initial);

    // The last NaN leaves +inf and the finite value: sum and avg are +inf.
    let inf = reducer.apply(&zset(&[(1, f64::NAN, -1)])).unwrap();
    assert_eq!(
        current(&inf),
        vec![(1, Some(f64::INFINITY), Some(f64::INFINITY))]
    );

    // Retract both +inf occurrences at once; the finite sum survives.
    let finite = reducer.apply(&zset(&[(1, f64::INFINITY, -2)])).unwrap();
    assert_eq!(current(&finite), vec![(1, Some(3.0), Some(3.0))]);
}

#[test]
fn over_retracting_a_special_is_rejected() {
    let mut reducer = reducer();
    reducer.apply(&zset(&[(1, f64::NAN, 1)])).unwrap();
    assert!(reducer.apply(&zset(&[(1, f64::NAN, -1)])).is_ok());
    assert!(
        reducer.apply(&zset(&[(1, f64::NAN, -1)])).is_err(),
        "retracting a special more often than inserted is invalid"
    );
}

#[test]
fn checkpoint_restore_keeps_special_state_then_retracts() {
    let mut source = reducer();
    source
        .apply(&zset(&[(1, f64::NAN, 1), (1, 2.0, 1)]))
        .unwrap();
    let state = source.export_state().unwrap();
    let bytes = crate::encode_framed(&state).unwrap();
    let decoded: hotlap_core::snapshot::GroupState = crate::decode_framed(&bytes).unwrap();

    let mut restored = reducer();
    restored.import_state(&decoded).unwrap();
    let out = restored.apply(&zset(&[(1, f64::NAN, -1)])).unwrap();
    assert_eq!(current(&out), vec![(1, Some(2.0), Some(2.0))]);
}

#[test]
fn checkpoint_restore_keeps_infinite_state_then_retracts() {
    let mut source = reducer();
    source
        .apply(&zset(&[
            (1, f64::INFINITY, 1),
            (1, f64::NEG_INFINITY, 1),
            (1, 5.0, 1),
        ]))
        .unwrap();
    let state = source.export_state().unwrap();
    let mut restored = reducer();
    restored.import_state(&state).unwrap();

    let out = restored
        .apply(&zset(&[(1, f64::INFINITY, -1), (1, f64::NEG_INFINITY, -1)]))
        .unwrap();
    assert_eq!(current(&out), vec![(1, Some(5.0), Some(5.0))]);
}
